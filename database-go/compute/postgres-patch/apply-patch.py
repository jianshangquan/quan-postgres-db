#!/usr/bin/env python3
"""Patch Postgres 16 source tree for Quan GetPage-native smgr (no local writes)."""
from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")


def _inject_after_brace(text: str, fn_name: str, inject: str) -> str:
    start = text.find(f"\n{fn_name}(")
    if start < 0:
        start = text.find(f"{fn_name}(")
    if start < 0:
        raise SystemExit(f"{fn_name} not found")
    brace = text.find("{", start)
    if brace < 0:
        raise SystemExit(f"{fn_name} body not found")
    return text[: brace + 1] + inject + text[brace + 1 :]


def patch_md_c() -> None:
    path = ROOT / "src/backend/storage/smgr/md.c"
    text = path.read_text()

    if '#include "quan_http.h"' not in text:
        if '#include "storage/md.h"' in text:
            text = text.replace(
                '#include "storage/md.h"\n',
                '#include "storage/md.h"\n#include "quan_http.h"\n',
                1,
            )
        else:
            text = '#include "quan_http.h"\n' + text

    if "Quan: GetPage-native" not in text:
        text = _inject_after_brace(
            text,
            "mdread",
            """
\t/* Quan: GetPage-native — pageserver only; no local fallthrough (local base/ may be empty). */
\tif (quan_remote_enabled() && !SmgrIsTemp(reln))
\t{
\t\tOid\t\t\tdboid = reln->smgr_rlocator.locator.dbOid;

\t\tif (!quan_getpage((uint32) dboid,
\t\t\t\t\t\t  (uint32) reln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t  (uint32) forknum,
\t\t\t\t\t\t  (uint32) blocknum,
\t\t\t\t\t\t  0,
\t\t\t\t\t\t  (char *) buffer))
\t\t\tereport(ERROR,
\t\t\t\t\t(errcode(ERRCODE_DATA_CORRUPTED),
\t\t\t\t\t errmsg("quan_getpage failed for db %u rel %u fork %u block %u",
\t\t\t\t\t\t\tdboid,
\t\t\t\t\t\t\treln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t\tforknum, blocknum)));
\t\treturn;
\t}
""",
        )
        print("patched mdread")

    if "quan_relsize" not in text:
        text = _inject_after_brace(
            text,
            "mdnblocks",
            """
\tif (quan_remote_enabled() && !SmgrIsTemp(reln))
\t{
\t\tOid\t\t\tdboid = reln->smgr_rlocator.locator.dbOid;
\t\tuint32\t\tn;

\t\tn = quan_relsize((uint32) dboid,
\t\t\t\t\t\t (uint32) reln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t (uint32) forknum,
\t\t\t\t\t\t 0);
\t\t/* Prefer remote size; 0 is valid for a brand-new empty relation. */
\t\treturn (BlockNumber) n;
\t}
""",
        )
        print("patched mdnblocks")

    # Skip local relation FileWrite / fsync when remote pageserver is enabled.
    # Pages go to pageserver via PutPage; durability still comes from WAL → safekeeper.
    if "Quan: skip-local-write" not in text:
        write_inject = """
\t/* Quan: skip-local-write — offload page body to pageserver; no local base/ write. */
\tif (quan_remote_enabled() && !SmgrIsTemp(reln))
\t{
\t\tOid\t\t\tdboid = reln->smgr_rlocator.locator.dbOid;

\t\tif (!quan_putpage((uint32) dboid,
\t\t\t\t\t\t\t(uint32) reln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t\t(uint32) forknum,
\t\t\t\t\t\t\t(uint32) blocknum,
\t\t\t\t\t\t\t0,
\t\t\t\t\t\t\t(const char *) buffer))
\t\t\tereport(ERROR,
\t\t\t\t\t(errcode(ERRCODE_IO_ERROR),
\t\t\t\t\t errmsg("quan_putpage failed for db %u rel %u fork %u block %u",
\t\t\t\t\t\t\tdboid,
\t\t\t\t\t\t\treln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t\tforknum, blocknum)));
\t\treturn;
\t}
"""
        text = _inject_after_brace(text, "mdwrite", write_inject)
        text = _inject_after_brace(text, "mdextend", write_inject)
        print("patched mdwrite/mdextend (skip-local-write)")

    if "Quan: skip-local-zeroextend" not in text:
        # PG16 bulk extend path — still touches local files unless skipped.
        text = _inject_after_brace(
            text,
            "mdzeroextend",
            """
\t/* Quan: skip-local-zeroextend — publish zero pages remotely; no local fallocate. */
\tif (quan_remote_enabled() && !SmgrIsTemp(reln))
\t{
\t\tOid\t\t\tdboid = reln->smgr_rlocator.locator.dbOid;
\t\tchar\t\tzeropage[BLCKSZ] = {0};
\t\tBlockNumber\ti;

\t\tfor (i = 0; i < (BlockNumber) nblocks; i++)
\t\t{
\t\t\tif (!quan_putpage((uint32) dboid,
\t\t\t\t\t\t\t\t(uint32) reln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t\t\t(uint32) forknum,
\t\t\t\t\t\t\t\t(uint32) (blocknum + i),
\t\t\t\t\t\t\t\t0,
\t\t\t\t\t\t\t\tzeropage))
\t\t\t\tereport(ERROR,
\t\t\t\t\t\t(errcode(ERRCODE_IO_ERROR),
\t\t\t\t\t\t errmsg("quan_putpage failed during zeroextend db %u rel %u block %u",
\t\t\t\t\t\t\t\tdboid,
\t\t\t\t\t\t\t\treln->smgr_rlocator.locator.relNumber,
\t\t\t\t\t\t\t\tblocknum + i)));
\t\t}
\t\treturn;
\t}
""",
        )
        print("patched mdzeroextend (skip-local-zeroextend)")

    if "Quan: skip-local-sync" not in text:
        sync_inject = """
\t/* Quan: skip-local-sync — no local segment fsync when pages live on pageserver. */
\tif (quan_remote_enabled() && !SmgrIsTemp(reln))
\t\treturn;
"""
        text = _inject_after_brace(text, "mdwriteback", sync_inject)
        text = _inject_after_brace(text, "mdimmedsync", sync_inject)
        print("patched mdwriteback/mdimmedsync (skip-local-sync)")

    path.write_text(text)
    print("patched md.c")


def patch_makefile() -> None:
    path = ROOT / "src/backend/storage/smgr/Makefile"
    text = path.read_text()
    if "quan_http.o" in text:
        print("Makefile already patched")
        return
    text = text.replace("OBJS = ", "OBJS = quan_http.o ", 1)
    path.write_text(text)
    print("patched smgr/Makefile")


def main() -> None:
    src = pathlib.Path(__file__).resolve().parent
    dest = ROOT / "src/backend/storage/smgr"
    for name in ("quan_http.c", "quan_http.h"):
        (dest / name).write_bytes((src / name).read_bytes())
        print(f"installed {name}")
    patch_md_c()
    patch_makefile()
    (ROOT / "quan-patch-applied").write_text("getpage-native+skip-local-write\n")


if __name__ == "__main__":
    main()
