/*-------------------------------------------------------------------------
 * quan_http.c
 *    libcurl client for pageserver GetPage / PutPage / GetRelSize HTTP API.
 *
 * Env:
 *   QUAN_PAGESERVER_HTTP  e.g. http://pageserver:8081
 *   TENANT_ID / TIMELINE_ID
 *   QUAN_REQUEST_LSN       0 = latest
 *-------------------------------------------------------------------------
 */
#include "quan_http.h"

#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Compiled into the Postgres backend; keep this file free of postgres.h so the
 * same sources can be unit-checked. Types come from quan_http.h (stdint). */

#define BLCKSZ_LOCAL 8192

struct mem {
	char *data;
	size_t len;
};

static size_t
write_cb(char *ptr, size_t size, size_t nmemb, void *userdata)
{
	struct mem *m = userdata;
	size_t n = size * nmemb;
	char *p = realloc(m->data, m->len + n + 1);
	if (!p)
		return 0;
	m->data = p;
	memcpy(m->data + m->len, ptr, n);
	m->len += n;
	m->data[m->len] = '\0';
	return n;
}

struct put_body {
	const char *data;
	size_t len;
	size_t off;
};

static size_t
read_cb(char *buffer, size_t size, size_t nitems, void *userdata)
{
	struct put_body *b = userdata;
	size_t want = size * nitems;
	size_t left = b->len - b->off;
	if (want > left)
		want = left;
	if (want == 0)
		return 0;
	memcpy(buffer, b->data + b->off, want);
	b->off += want;
	return want;
}

static void
quan_curl_once(void)
{
	static int inited = 0;
	if (!inited)
	{
		curl_global_init(CURL_GLOBAL_DEFAULT);
		inited = 1;
	}
}

bool quan_remote_enabled(void) {
	const char *u = getenv("QUAN_PAGESERVER_HTTP");
	return u != NULL && u[0] != '\0';
}

static const char * tenant(void) {
	const char *t = getenv("TENANT_ID");
	return t && t[0] ? t : "default";
}

static const char * timeline(void) {
	const char *t = getenv("TIMELINE_ID");
	return t && t[0] ? t : "main";
}

static uint64_t req_lsn(uint64_t override_lsn) {
	if (override_lsn != 0)
		return override_lsn;
	const char *e = getenv("QUAN_REQUEST_LSN");
	if (e && e[0])
		return strtoull(e, NULL, 10);
	return 0; /* pageserver treats 0 as latest */
}

bool quan_getpage(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint32_t block, uint64_t request_lsn, char *out8192) {
	const char *base = getenv("QUAN_PAGESERVER_HTTP");
	if (!base || !base[0])
		return false;

	quan_curl_once();

	char url[1024];
	snprintf(url, sizeof(url),
			 "%s/v1/page?tenant=%s&timeline=%s&db=%u&rel=%u&fork=%u&block=%u&lsn=%llu",
			 base, tenant(), timeline(), db_oid, rel_filenode, fork, block,
			 (unsigned long long)req_lsn(request_lsn));

	CURL *curl = curl_easy_init();
	if (!curl)
		return false;

	struct mem m = {0};
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &m);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	{
		const char *tok = getenv("QUAN_STORAGE_TOKEN");
		struct curl_slist *hdrs = NULL;
		if (tok && tok[0])
		{
			char h1[512], h2[512];
			snprintf(h1, sizeof(h1), "Authorization: Bearer %s", tok);
			snprintf(h2, sizeof(h2), "X-Quan-Token: %s", tok);
			hdrs = curl_slist_append(hdrs, h1);
			hdrs = curl_slist_append(hdrs, h2);
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
		}
		CURLcode rc = curl_easy_perform(curl);
		long code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (hdrs)
			curl_slist_free_all(hdrs);
		curl_easy_cleanup(curl);

		if (rc != CURLE_OK || code != 200 || m.len != BLCKSZ_LOCAL)
		{
			free(m.data);
			return false;
		}
		memcpy(out8192, m.data, BLCKSZ_LOCAL);
		free(m.data);
		return true;
	}
}

bool quan_putpage(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint32_t block, uint64_t page_lsn, const char *page8192) {
	const char *base = getenv("QUAN_PAGESERVER_HTTP");
	if (!base || !base[0] || !page8192)
		return false;

	quan_curl_once();

	/* Never inherit QUAN_REQUEST_LSN for writes — that pin is for GetPage only.
	 * Use a process-local monotonic LSN so later mdwrite beats mdextend zeros. */
	static uint64_t put_seq = 1;
	uint64_t lsn = page_lsn;
	if (lsn == 0)
		lsn = put_seq++;

	char url[1024];
	snprintf(url, sizeof(url),
			 "%s/v1/page?tenant=%s&timeline=%s&db=%u&rel=%u&fork=%u&block=%u&lsn=%llu",
			 base, tenant(), timeline(), db_oid, rel_filenode, fork, block,
			 (unsigned long long)lsn);

	CURL *curl = curl_easy_init();
	if (!curl)
		return false;

	struct mem m = {0};
	struct put_body body = {.data = page8192, .len = BLCKSZ_LOCAL, .off = 0};
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_cb);
	curl_easy_setopt(curl, CURLOPT_READDATA, &body);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)BLCKSZ_LOCAL);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &m);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	{
		const char *tok = getenv("QUAN_STORAGE_TOKEN");
		struct curl_slist *hdrs = NULL;
		hdrs = curl_slist_append(hdrs, "Content-Type: application/octet-stream");
		hdrs = curl_slist_append(hdrs, "Expect:"); /* disable 100-continue */
		if (tok && tok[0])
		{
			char h1[512], h2[512];
			snprintf(h1, sizeof(h1), "Authorization: Bearer %s", tok);
			snprintf(h2, sizeof(h2), "X-Quan-Token: %s", tok);
			hdrs = curl_slist_append(hdrs, h1);
			hdrs = curl_slist_append(hdrs, h2);
		}
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
		CURLcode rc = curl_easy_perform(curl);
		long code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (hdrs)
			curl_slist_free_all(hdrs);
		curl_easy_cleanup(curl);
		free(m.data);
		if (rc != CURLE_OK || code != 200)
			fprintf(stderr, "quan_putpage: curl=%d http=%ld url=%s\n", (int)rc, code, url);
		return rc == CURLE_OK && code == 200;
	}
}

uint32_t quan_relsize(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint64_t request_lsn){
	const char *base = getenv("QUAN_PAGESERVER_HTTP");
	if (!base || !base[0])
		return 0;

	quan_curl_once();

	char url[1024];
	snprintf(url, sizeof(url),
			 "%s/v1/relsize?tenant=%s&timeline=%s&db=%u&rel=%u&fork=%u&lsn=%llu",
			 base, tenant(), timeline(), db_oid, rel_filenode, fork,
			 (unsigned long long)req_lsn(request_lsn));

	CURL *curl = curl_easy_init();
	if (!curl)
		return 0;

	struct mem m = {0};
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &m);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
	{
		const char *tok = getenv("QUAN_STORAGE_TOKEN");
		struct curl_slist *hdrs = NULL;
		if (tok && tok[0])
		{
			char h1[512], h2[512];
			snprintf(h1, sizeof(h1), "Authorization: Bearer %s", tok);
			snprintf(h2, sizeof(h2), "X-Quan-Token: %s", tok);
			hdrs = curl_slist_append(hdrs, h1);
			hdrs = curl_slist_append(hdrs, h2);
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
		}
		CURLcode rc = curl_easy_perform(curl);
		long code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (hdrs)
			curl_slist_free_all(hdrs);
		curl_easy_cleanup(curl);

		if (rc != CURLE_OK || code != 200 || !m.data)
		{
			free(m.data);
			return 0;
		}
		uint32_t n = 0;
		const char *p = strstr(m.data, "nblocks");
		if (p)
		{
			p = strchr(p, ':');
			if (p)
				n = (uint32_t)strtoul(p + 1, NULL, 10);
		}
		free(m.data);
		return n;
	}
}
