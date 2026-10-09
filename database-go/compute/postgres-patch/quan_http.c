/*-------------------------------------------------------------------------
 * quan_http.c
 *    libcurl client for pageserver GetPage / PutPage / GetRelSize HTTP API.
 *
 * Env:
 *   QUAN_PAGESERVER_HTTP_URLS  comma-separated bases (failover)
 *   QUAN_PAGESERVER_HTTP       single base (fallback)
 *   TENANT_ID / TIMELINE_ID
 *   QUAN_REQUEST_LSN           0 = latest (GetPage/RelSize only)
 *   QUAN_STORAGE_TOKEN
 *   QUAN_TLS_CA / TLS_CA_FILE  PEM CA for HTTPS
 *-------------------------------------------------------------------------
 */
#include "quan_http.h"

#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLCKSZ_LOCAL 8192
#define MAX_URLS 8
#define URL_LEN 512

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

static const char *
tenant(void)
{
	const char *t = getenv("TENANT_ID");
	return t && t[0] ? t : "default";
}

static const char *
timeline(void)
{
	const char *t = getenv("TIMELINE_ID");
	return t && t[0] ? t : "main";
}

static uint64_t
req_lsn(uint64_t override_lsn)
{
	if (override_lsn != 0)
		return override_lsn;
	const char *e = getenv("QUAN_REQUEST_LSN");
	if (e && e[0])
		return strtoull(e, NULL, 10);
	return 0;
}

static int sticky_idx = 0;

static int
parse_bases(char bases[][URL_LEN], int max)
{
	const char *multi = getenv("QUAN_PAGESERVER_HTTP_URLS");
	const char *single = getenv("QUAN_PAGESERVER_HTTP");
	char buf[4096];
	int n = 0;

	if (multi && multi[0])
		snprintf(buf, sizeof(buf), "%s", multi);
	else if (single && single[0])
		snprintf(buf, sizeof(buf), "%s", single);
	else
		return 0;

	char *p = buf;
	while (*p && n < max)
	{
		while (*p == ' ' || *p == ',')
			p++;
		if (!*p)
			break;
		char *start = p;
		while (*p && *p != ',')
			p++;
		size_t len = (size_t)(p - start);
		while (len > 0 && start[len - 1] == ' ')
			len--;
		if (len == 0 || len >= URL_LEN)
			continue;
		memcpy(bases[n], start, len);
		bases[n][len] = '\0';
		/* trim trailing slash */
		while (len > 0 && bases[n][len - 1] == '/')
		{
			bases[n][--len] = '\0';
		}
		n++;
	}
	if (sticky_idx >= n)
		sticky_idx = 0;
	return n;
}

bool
quan_remote_enabled(void)
{
	char bases[MAX_URLS][URL_LEN];
	return parse_bases(bases, MAX_URLS) > 0;
}

static void
apply_tls(CURL *curl)
{
	const char *ca = getenv("QUAN_TLS_CA");
	if (!ca || !ca[0])
		ca = getenv("TLS_CA_FILE");
	if (ca && ca[0])
		curl_easy_setopt(curl, CURLOPT_CAINFO, ca);
}

static struct curl_slist *
auth_headers(struct curl_slist *hdrs)
{
	const char *tok = getenv("QUAN_STORAGE_TOKEN");
	if (tok && tok[0])
	{
		char h1[512], h2[512];
		snprintf(h1, sizeof(h1), "Authorization: Bearer %s", tok);
		snprintf(h2, sizeof(h2), "X-Quan-Token: %s", tok);
		hdrs = curl_slist_append(hdrs, h1);
		hdrs = curl_slist_append(hdrs, h2);
	}
	return hdrs;
}

bool
quan_getpage(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint32_t block, uint64_t request_lsn, char *out8192)
{
	char bases[MAX_URLS][URL_LEN];
	int n = parse_bases(bases, MAX_URLS);
	if (n <= 0)
		return false;

	quan_curl_once();
	int start = sticky_idx;
	for (int i = 0; i < n; i++)
	{
		int idx = (start + i) % n;
		char url[1024];
		snprintf(url, sizeof(url),
				 "%s/v1/page?tenant=%s&timeline=%s&db=%u&rel=%u&fork=%u&block=%u&lsn=%llu",
				 bases[idx], tenant(), timeline(), db_oid, rel_filenode, fork, block,
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
		apply_tls(curl);
		struct curl_slist *hdrs = auth_headers(NULL);
		if (hdrs)
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
		CURLcode rc = curl_easy_perform(curl);
		long code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (hdrs)
			curl_slist_free_all(hdrs);
		curl_easy_cleanup(curl);

		if (rc == CURLE_OK && code == 200 && m.len == BLCKSZ_LOCAL)
		{
			memcpy(out8192, m.data, BLCKSZ_LOCAL);
			free(m.data);
			sticky_idx = idx;
			return true;
		}
		free(m.data);
	}
	return false;
}

bool
quan_putpage(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint32_t block, uint64_t page_lsn, const char *page8192)
{
	char bases[MAX_URLS][URL_LEN];
	int n = parse_bases(bases, MAX_URLS);
	if (n <= 0 || !page8192)
		return false;

	quan_curl_once();

	static uint64_t put_seq = 1;
	uint64_t lsn = page_lsn;
	if (lsn == 0)
		lsn = put_seq++;

	int start = sticky_idx;
	for (int i = 0; i < n; i++)
	{
		int idx = (start + i) % n;
		char url[1024];
		snprintf(url, sizeof(url),
				 "%s/v1/page?tenant=%s&timeline=%s&db=%u&rel=%u&fork=%u&block=%u&lsn=%llu",
				 bases[idx], tenant(), timeline(), db_oid, rel_filenode, fork, block,
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
		apply_tls(curl);

		struct curl_slist *hdrs = NULL;
		hdrs = curl_slist_append(hdrs, "Content-Type: application/octet-stream");
		hdrs = curl_slist_append(hdrs, "Expect:");
		hdrs = auth_headers(hdrs);
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
		CURLcode rc = curl_easy_perform(curl);
		long code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (hdrs)
			curl_slist_free_all(hdrs);
		curl_easy_cleanup(curl);
		free(m.data);

		if (rc == CURLE_OK && code == 200)
		{
			sticky_idx = idx;
			return true;
		}
		fprintf(stderr, "quan_putpage: curl=%d http=%ld url=%s\n", (int)rc, code, url);
	}
	return false;
}

uint32_t
quan_relsize(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint64_t request_lsn)
{
	char bases[MAX_URLS][URL_LEN];
	int n = parse_bases(bases, MAX_URLS);
	if (n <= 0)
		return 0;

	quan_curl_once();
	int start = sticky_idx;
	for (int i = 0; i < n; i++)
	{
		int idx = (start + i) % n;
		char url[1024];
		snprintf(url, sizeof(url),
				 "%s/v1/relsize?tenant=%s&timeline=%s&db=%u&rel=%u&fork=%u&lsn=%llu",
				 bases[idx], tenant(), timeline(), db_oid, rel_filenode, fork,
				 (unsigned long long)req_lsn(request_lsn));

		CURL *curl = curl_easy_init();
		if (!curl)
			return 0;

		struct mem m = {0};
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &m);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
		apply_tls(curl);
		struct curl_slist *hdrs = auth_headers(NULL);
		if (hdrs)
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
		CURLcode rc = curl_easy_perform(curl);
		long code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
		if (hdrs)
			curl_slist_free_all(hdrs);
		curl_easy_cleanup(curl);

		if (rc == CURLE_OK && code == 200 && m.data)
		{
			uint32_t nblocks = 0;
			const char *p = strstr(m.data, "nblocks");
			if (p)
			{
				p = strchr(p, ':');
				if (p)
					nblocks = (uint32_t)strtoul(p + 1, NULL, 10);
			}
			free(m.data);
			sticky_idx = idx;
			return nblocks;
		}
		free(m.data);
	}
	return 0;
}
