/*-------------------------------------------------------------------------
 * quan_http.h
 *    Minimal HTTP GetPage client for Quan ERP from-scratch pageserver.
 *-------------------------------------------------------------------------
 */
#ifndef QUAN_HTTP_H
#define QUAN_HTTP_H

#include <stdbool.h>
#include <stdint.h>

/* Fetch one 8192-byte page. Returns true on success. */
bool quan_getpage(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork,
				  uint32_t block, uint64_t request_lsn, char *out8192);

/* Upload one 8192-byte page (skip local md write path). */
bool quan_putpage(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork, uint32_t block, uint64_t page_lsn, const char *page8192);

/* Relation size in blocks (0 if unknown). */
uint32_t quan_relsize(uint32_t db_oid, uint32_t rel_filenode, uint32_t fork,
					  uint64_t request_lsn);

/* True when QUAN_PAGESERVER_HTTP is set. */
bool quan_remote_enabled(void);

#endif
