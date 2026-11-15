#!/usr/bin/env python3
"""Assert krn_engine has not acquired a data-layer dependency.  (ENC-1481)

Design: specs/2026-10-14-krn-data-layer/SPEC.md D14;
        krn-data's docs/data-layer.md §4 rule 6.

THE INVARIANT, in the wording that actually matters:

    krn_engine must not acquire a dependency on Arrow, the ClickHouse client,
    or the krn_data library target.

Not "no krn/data header is reachable from engine code" -- that phrasing is
wrong, and the distinction is load-bearing.  `include/krn/engine/SymbolTable.hpp`
lives inside the engine's own include tree on purpose (ENC-1486's record
ingress resolves a symbolId through it, and Dispatcher IS engine code).  What
the engine must not be able to name is Arrow, or a header that exists only
under `data/include` -- since ENC-1557 that includes the record PODs.

WHY THIS IS A SCRIPT AND NOT A SENTENCE IN A COMMIT MESSAGE
This is the invariant in the design most likely to rot silently, because every
way of breaking it still compiles and still passes every existing test.  Adding
`krn::arrow` to krn_engine's link line makes the LIVE-ONLY VARIANT of SPEC
D14's client profile impossible, and nothing anywhere would have noticed.
(D14's wording is "live-only *or* reads a central L1 over the wire with no
local store".  The second variant does ship krn_data -- it needs Arrow IPC --
so what the invariant protects is the first one, plus the general property that
krn_engine builds whether or not the data layer is in the build.  "The client
profile omits krn_data" is true of one variant and not of D14.)

THE CHECKS, each closing a different route in:

  A  textual   no engine translation unit includes an Arrow / Parquet /
               ClickHouse / DuckDB header, nor a header that exists only under
               `connectors/*/include` or `plugins/*/include`.
  B  include   krn_engine's include directories, restricted to paths inside
               the repo, are `<root>/include` or a VENDORED THIRD-PARTY tree
               under `<root>/third_party` whose name is not itself a forbidden
               dependency.  See CHECK B IS THE WEAKEST OF THE FOUR below.
  C  link      nothing matching krn_data / arrow / parquet / clickhouse is
               reachable from krn_engine's link libraries — TRANSITIVELY.
  D  profile   nothing matching them is reachable from krn_host's or
               krn_engine_server's either, which is what makes the LIVE-ONLY
               VARIANT of SPEC D14's client profile actually buildable rather
               than nominally so.  `krn_tests` is checked on the same rule,
               because krn-data's data/CMakeLists.txt names "the engine test
               suite is built against an Arrow-free include path" as the thing
               being protected.  No graphed target may reach
               krn_connector_market: it and krn_server are krn-common's since
               ENC-1570, so the ban is no longer per root (ENC-1561, ENC-1562).

  E  lexical   (purity SPEC D11.3; ENC-1578 the stripper, ENC-1579 the
               vocabulary) no identifier in check A's own file list, with
               comments and literal bodies blanked, is market vocabulary
               derived from git history (tools/derive_market_vocabulary.py).
               Its control is a committed fixture, scanned every run.

  F  artifact  (ENC-1491; `--artifacts`, ctest krn_engine_isolation_artifact)
               `nm -C` over the BUILT libkrn_engine.a, libkrn_host.a and
               krn_engine_server finds no Arrow/Parquet/Gandiva/ClickHouse/
               DuckDB symbol (rules derived from FORBIDDEN_INCLUDE_PREFIXES),
               no krn_data symbol and no market symbol (check E's vocabulary).
               A-E read the source and the graph; F asks the linker.  Its
               control is krn_isolation_nm_control, a compiled fixture.  See
               the block above ARTIFACT_ROOTS.

CHECK B IS THE WEAKEST OF THE FOUR, AND ITS HOLE IS DELIBERATE.  (ENC-1497)
Its rule used to be "any in-repo include dir that is not `<root>/include`",
which is an over-approximation of the invariant: a vendored third-party include
tree is not Arrow, not the ClickHouse client and not krn_data, and rapidjson is
a long-standing *engine* dependency -- `krn::Event` holds a
`shared_ptr<rapidjson::Document>` and `src/core/Dispatcher.cpp` has always
walked a DOM.  `/third_party/` is gitignored, so it is populated in the shared
checkout and absent in a fresh `git worktree`; CMake's `find_path` therefore
resolves RapidJSON to `<root>/third_party/rapidjson` in one and to
`/usr/include` in the other, and THE SAME COMMIT FAILED FROM ONE CHECKOUT AND
PASSED FROM THE OTHER.  The over-approximation was never exercised because this
gate was authored in a worktree where `third_party/` happened to be empty.

A false positive costs as much as a false negative here: a gate that reddens on
a legitimate configuration gets routed around, and this one guards a decision
(D14's live-only client profile) that nothing else checks.  So B accepts both
resolutions -- but the exemption is BOUNDED, because an exemption with no
stated bound is how this comes back:

  * only `<root>/third_party/...` is exempt, and only after `os.path.realpath`,
    so `data/include` symlinked under `third_party/` resolves back out of the
    exemption and still fails;
  * a vendored tree whose path names a forbidden dependency -- arrow, parquet,
    gandiva, clickhouse, duckdb, krn_data -- still FAILS, so the hole cannot be
    used to put Arrow on the engine's include path;
  * `data/include` and both connector include roots still fail, which is check
    B's actual job.

What B is NOT is the load-bearing check.  Vendoring Arrow under some
unrecognised name would get past it; it would not get past A (no engine
translation unit may include an Arrow/Parquet/ClickHouse/DuckDB header, matched
on the include text, not on a directory name) or C/D (nothing matching those
names anywhere in the TRANSITIVE link closure of krn_engine, krn_host,
krn_engine_server or krn_tests).  B is a cheap structural sanity check on one target property; A and
C/D carry the invariant.  Read a B failure as "the engine's include path grew
something it should not have", never as the only thing standing between the
engine and Arrow.

WHY THE RESOLVED RAPIDJSON PATH IS REPORTED.  The underlying defect was not
that one verdict was wrong -- it was that two checkouts produced DIFFERENT
verdicts with nothing in the output to explain why.  So the manifest carries
`rapidjson_include_dir` (CMake already `message(STATUS ...)`s it) and the
summary prints it with whether it is in-repo, vendored or external.  A
differing verdict between two checkouts of the same commit must be explicable
from the gate's own output.

C AND D WALK THE CLOSURE, AND HAVE TO.  They read a *link graph* out of the
manifest, not one target's own property, because the direct property is the
wrong place to look.  `krn_engine_server` links `krn_host` and the reference
plugin (`link.krn_engine_server=krn_host|krn_plugin_reference`), so a single
`target_link_libraries(krn_host PUBLIC krn_data)` -- or `krn::arrow` on the
plugin -- makes the live-only variant unlinkable while krn_engine_server's OWN
property stays spotless.  That is precisely the "one line and nothing would
say so" failure this file exists to close, so a one-level check does not close
it.  CMake emits one row per first-party target (`link.<target>` /
`interface_link.<target>`); anything not named by a row is a leaf and is only
pattern-matched.  The selftest pins both the intermediate (krn_host) case and, as a labelled
contrast, the fact that a one-level read would have passed it.

POSITIVE CONTROL.  (ENC-1507, purity SPEC Q7.)  The root CMakeLists declares
`krn_isolation_control`, an INTERFACE target linking an IMPORTED stub spelled
CONTROL_PROBE below, which the existing `arrow` pattern matches.  It has a
manifest row like any first-party target, and its closure MUST trip a
forbidden pattern and reach the stub.  If it does not, the patterns match
nothing in this build and a clean verdict about krn_engine is a verdict from a
search that cannot find anything.  It is declared in this repo, and since
data/ left for krn-data (ENC-1573) it is the only control.  It is scanned,
never linked: any other target whose closure reaches it is a violation.

EXIT CODES.  0 verified pass; 1 a violation was found; 2 could not run --
including vacuous input and a missing or silent control.  "Could not check"
and "checked and clean" are never the same exit code.  A violation outranks
vacuity: a found violation is a finding whatever else is missing.

THERE IS NO COMPOSITION ROOT IN src/ ANY MORE.  `src/main.cpp` was krn_server's
and was checked by a weaker rule; it left with krn_server for krn-common
(ENC-1570), so everything under src/ and include/ is engine code again.

`apps/` (krn_engine_server's sources, ENC-1561) is scanned by a middle rule:
it composes the reference plugin, so `plugins/*/include` is allowed, but it is
the connector-free server, so `connectors/*/include` is not.  With no
`connectors/` in this repo its foreign-root list is EMPTY, on purpose (see
app_foreign_roots): the forbidden include prefixes still apply to apps/, and a
connector re-added under `connectors/` is enumerated and barred again.

The file list for check A comes from CMake's own
`$<TARGET_PROPERTY:krn_engine,SOURCES>`, not from this script re-deriving the
glob.  A second derivation of the same set is a second thing to keep in sync,
and the one that drifts is always the copy.  The filesystem walk is kept as a
cross-check in the other direction: it fails if krn_engine compiles a file from
outside src/ and include/.

ANTI-VACUITY.  Every check asserts its own input is non-empty.  A scan of zero
files and a property list of zero entries both pass every predicate above while
proving nothing, and that reads exactly like a clean run -- the failure mode
this repo has been bitten by more than once (ENC-1289's expected-failure
machinery, the lint anti-vacuity rule in krn-data's docs/data-layer.md §5.4, and the
workspace-root grep that reaches none of the seven repos).  An empty input is
an ERROR here, not a pass.

SELF-TEST.  `--selftest` runs each predicate against an input that violates it
and fails unless the violation is detected.  So the claim "this check would
catch it" is itself checked, every ctest run, rather than being a claim.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

# Headers that must never be reachable from engine code.  Matched as a prefix
# of the quoted include path.
FORBIDDEN_INCLUDE_PREFIXES = (
    "arrow/",
    "arrow_",
    "parquet/",
    "gandiva/",
    "clickhouse/",
    "clickhouse-cpp/",
    "duckdb",
)

# Library names that must never appear in krn_engine's link libraries.
FORBIDDEN_LINK_PATTERNS = (
    re.compile(r"(?:^|[:/])krn_data$"),
    re.compile(r"arrow", re.IGNORECASE),
    re.compile(r"parquet", re.IGNORECASE),
    re.compile(r"clickhouse", re.IGNORECASE),
    re.compile(r"duckdb", re.IGNORECASE),
)

# The market connector (purity SPEC D11.1).  It left this repo with krn_server
# and krn_market_tests (ENC-1570), so there is no market half: NO graphed
# target may reach it, checked root or not (market_reachers).  A name is
# enough -- the real target is gone, so a link to one is an IMPORTED stub or a
# re-added target, and either is the violation.
MARKET_CONNECTOR_LINK = re.compile(r"(?:^|[:/])krn_connector_market$")

# The checked roots (ENC-1561, D11.2).  All but krn_tests must have a row.
CHECKED_ROOTS = ("krn_engine", "krn_host", "krn_engine_server", "krn_tests")
REQUIRED_ROOTS = ("krn_engine", "krn_host", "krn_engine_server")

# The positive control (ENC-1507).  Must match root CMakeLists.txt.
CONTROL_TARGET = "krn_isolation_control"
CONTROL_PROBE = "arrow::arrow_control_probe"

INCLUDE_RE = re.compile(r'^\s*#\s*include\s*[<"]([^>"]+)[>"]')

# The one subtree of the repo whose include dirs check B permits on
# krn_engine's include path, and the names that are still refused inside it.
# (ENC-1497 -- see CHECK B IS THE WEAKEST OF THE FOUR in the docstring.)
#
# Matched with `search` against the path RELATIVE TO third_party/, so a release
# tarball directory (`apache-arrow-18.0.0/cpp/src`) is caught as well as a bare
# `arrow`.  Same vocabulary as FORBIDDEN_INCLUDE_PREFIXES and
# FORBIDDEN_LINK_PATTERNS; it is spelled out separately rather than derived
# because those two match a quoted include path and a library name
# respectively, and a directory name is neither.  ADD A NEW FORBIDDEN
# DEPENDENCY TO ALL THREE.
VENDOR_SUBDIR = "third_party"
FORBIDDEN_VENDOR_RE = re.compile(
    r"arrow|parquet|gandiva|clickhouse|duckdb|krn_data", re.IGNORECASE)

# Floors for the anti-vacuity assertions.  The margin has to be real, not
# claimed: the engine was **99** source/header files once src/main.cpp was
# excluded (measured 2026-10-14 -- `find src include -type f` is 101, of which
# one is src/util/krn.conf and one is the composition root, and the gate prints
# the 99 it actually scanned).  The 50 that stood here was advertised as "well
# under the real count (the engine was ~190 files)"; the engine has never been
# 190 files, and the 190's provenance is unknown -- the whole repo is **231**
# .cpp/.hpp files (`git ls-files '*.cpp' '*.hpp' | wc -l`, and the same at the
# commit that wrote the 190), and no subtree of this repo comes to 190.  So the
# floor was half the true number under a comment that cited a figure matching
# nothing.  80 is a ~19% margin: enough for ordinary deletion, tight enough
# that a walk finding half the engine fails.
#
# It bounds the WALK and the union separately, in main(), and the distinction
# is the whole value of the number: `sources` is the union of the walk and
# CMake's SOURCES property, so CMake alone supplies 99 and ANY floor below 99
# on the union is clear even when the walk returns nothing.  A floor on the
# union is therefore not a check on the walk -- raising 50 to 80 buys nothing
# by itself, which is why the walk is now floored directly.
MIN_ENGINE_FILES = 80
MIN_ENGINE_INCLUDE_DIRS = 1


def read_manifest(path: str) -> dict[str, str]:
    """Parse the key=value file CMake generates with the resolved properties."""
    out: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            out[key.strip()] = value.strip()
    return out


def split_list(value: str) -> list[str]:
    # CMake's $<JOIN:...,|> output.  An empty property yields "".
    return [part for part in (p.strip() for p in value.split("|")) if part]


def dedupe(items: list[str]) -> list[str]:
    """Order-preserving dedupe.

    INCLUDE_DIRECTORIES and INTERFACE_INCLUDE_DIRECTORIES overlap almost
    entirely for a PUBLIC include dir, so without this every include-path
    violation is reported twice and the count in the summary is wrong.
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


SOURCE_SUFFIXES = (".cpp", ".hpp", ".h", ".cc", ".cxx", ".ipp")

# krn_engine_server's tree (ENC-1561).
APPS_DIR = "apps"


def walk_tree(root: str, sub: str) -> list[str]:
    """Every source/header under `root/sub`, as realpaths."""
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(root, sub)):
        dirnames[:] = [d for d in dirnames if d not in (".git", "build")]
        for name in filenames:
            if name.endswith(SOURCE_SUFFIXES):
                found.append(os.path.realpath(os.path.join(dirpath, name)))
    return sorted(found)


def walk_engine_trees(root: str) -> list[str]:
    """Every source/header under the two trees krn_engine is globbed from.

    Not apps/, plugins/ or tests/.  Nothing is excluded: src/main.cpp left
    with krn_server (ENC-1570), so a file of that name is engine code.
    """
    return sorted(walk_tree(root, "src") + walk_tree(root, "include"))


def check_cmake_sources_stay_in_the_engine_trees(
        root: str, cmake_sources: list[str], walked: set[str],
        target: str = "krn_engine") -> list[str]:
    """Nothing from outside src/ and include/ is compiled into krn_engine.

    The other direction of check A: if krn_engine ever started compiling a file
    from data/ or connectors/, the walk above would never see it and A would
    pass while the layering had already been broken.
    """
    problems: list[str] = []
    for src in cmake_sources:
        real = os.path.realpath(src)
        if not real.endswith(SOURCE_SUFFIXES):
            continue
        if real not in walked:
            problems.append(
                f"{target} compiles {os.path.relpath(real, root)}, which is "
                f"outside src/ and include/ -- engine sources must come from "
                f"the engine's own trees")
    return problems


def non_engine_header_roots(root: str) -> list[str]:
    """Include roots that belong to the connectors and the plugins.

    Enumerated, never named.  `data/include` was a named root, filtered out
    by the isdir test once ENC-1573 deleted data/, which left the
    composition-root rule checking an empty list in silence (ENC-1578).
    """
    roots = []
    for parent in ("connectors", "plugins"):
        base = os.path.join(root, parent)
        if not os.path.isdir(base):
            continue
        for name in sorted(os.listdir(base)):
            candidate = os.path.join(base, name, "include")
            if os.path.isdir(candidate):
                roots.append(candidate)
    return [r for r in roots if os.path.isdir(r)]


def app_foreign_roots(root: str, foreign: list[str]) -> list[str]:
    """The roots apps/ may not reach: `foreign` minus plugins/*/include.

    EMPTY IN THIS REPO, AND THAT IS INTENDED (ENC-1570).  It is every
    `connectors/*/include`, and connectors/market left for krn-common.  Kept
    rather than dropped because it is enumerated: a connector re-added under
    connectors/ is barred from apps/ with no edit here, which check E's
    vocabulary does not cover (its scope is src/ and include/).  The selftest pins both the empty
    list and that the forbidden include prefixes still bite on apps/.
    """
    plugins = os.path.realpath(os.path.join(root, "plugins"))
    return [r for r in foreign if not _under(os.path.realpath(r), plugins)]


def check_a_includes(root: str, sources: list[str], foreign_roots: list[str],
                     label: str = "engine code") -> list[str]:
    """No file in `sources` includes a forbidden header or a foreign-only one.

    `foreign_roots` is the set of include trees this file may not reach into.
    For engine proper that is every connector and plugin; for apps/ it is
    every connector (app_foreign_roots).
    """
    engine_include = os.path.join(root, "include")
    problems: list[str] = []
    for path in sources:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError as exc:  # pragma: no cover - unreadable file is a problem
            problems.append(f"{path}: cannot read ({exc})")
            continue
        for lineno, line in enumerate(lines, start=1):
            m = INCLUDE_RE.match(line)
            if not m:
                continue
            inc = m.group(1)
            low = inc.lower()
            for bad in FORBIDDEN_INCLUDE_PREFIXES:
                if low.startswith(bad):
                    problems.append(
                        f"{os.path.relpath(path, root)}:{lineno}: {label} "
                        f"includes <{inc}> -- Arrow/ClickHouse must never become "
                        f"a krn_engine or krn_engine_server dependency (SPEC D14)")
                    break
            else:
                # A header that resolves under data/ or a connector but NOT
                # under the engine's own include/ is a layering violation.
                # `krn/engine/SymbolTable.hpp` resolves under include/, so it passes;
                # `krn/data/Schema.hpp` does not, so it does not.
                if os.path.exists(os.path.join(engine_include, inc)):
                    continue
                for froot in foreign_roots:
                    if os.path.exists(os.path.join(froot, inc)):
                        problems.append(
                            f"{os.path.relpath(path, root)}:{lineno}: {label} "
                            f"includes \"{inc}\", which exists only under "
                            f"{os.path.relpath(froot, root)}/ -- the engine may not "
                            f"depend on krn_data or on a connector")
                        break
    return problems


def _under(path: str, base: str) -> bool:
    """`path` is `base` or lies beneath it.  Both must already be realpaths.

    Written out rather than inlined because the naive `startswith(base)` makes
    `third_party_evil/` a sibling that passes as `third_party/`, and check B's
    exemption is exactly where that bug would be load-bearing.  The selftest
    pins the sibling case.
    """
    return path == base or path.startswith(base + os.sep)


def check_b_include_dirs(root: str, dirs: list[str]) -> list[str]:
    """krn_engine's in-repo include dirs are <root>/include or vendored.

    The weakest of the four checks, deliberately so, and bounded -- read CHECK
    B IS THE WEAKEST OF THE FOUR in the module docstring before widening it.
    Permitted, for an include dir that resolves inside the repo:

      * `<root>/include`, which must be present;
      * anything under `<root>/third_party/` whose relative path does not name
        a forbidden dependency (ENC-1497).

    Everything else in-repo fails, and that set is the check's actual job:
    `data/include`, `connectors/market/include`, `plugins/reference/include`.
    Comparison is on `os.path.realpath`, so the exemption cannot be reached by
    symlinking a foreign include root into `third_party/` -- it resolves back
    out and fails on the general rule.
    """
    allowed = os.path.realpath(os.path.join(root, "include"))
    vendor = os.path.realpath(os.path.join(root, VENDOR_SUBDIR))
    real_root = os.path.realpath(root)
    problems: list[str] = []
    in_repo: list[str] = []
    for d in dirs:
        real = os.path.realpath(d)
        if _under(real, real_root):
            in_repo.append(real)
    for real in in_repo:
        if _under(real, allowed):
            continue
        rel = os.path.relpath(real, root)
        # The bounded exemption.  `vendor` is only inside the repo when
        # third_party/ is a real directory there; if it is a symlink pointing
        # out, nothing under it reaches this loop in the first place.
        if _under(real, vendor) and _under(vendor, real_root):
            under_vendor = os.path.relpath(real, vendor)
            hit = FORBIDDEN_VENDOR_RE.search(under_vendor)
            if hit is None:
                continue
            problems.append(
                f"krn_engine include directory {rel} is a vendored tree whose "
                f"path names '{hit.group(0)}' -- {VENDOR_SUBDIR}/ is exempt from "
                f"the include-path rule so that a vendored rapidjson resolves "
                f"the same way in the shared checkout and in a worktree "
                f"(ENC-1497), but Arrow, Parquet, Gandiva, the ClickHouse "
                f"client, DuckDB and krn_data are exactly what that exemption "
                f"may not be used to carry (SPEC D14)")
            continue
        problems.append(
            f"krn_engine include directory {rel} is "
            f"inside the repo but is not include/ -- the engine's include "
            f"path must expose engine headers only (a vendored third-party "
            f"tree under {VENDOR_SUBDIR}/ is the one exception)")
    if allowed not in in_repo:
        problems.append(
            "krn_engine's include directories do not contain include/ at all; "
            "the manifest is wrong or the target was restructured")
    return problems


def classify_include_dir(root: str, path: str) -> str:
    """Where `path` sits relative to the repo, in words, for the summary.

    THE DEFECT THIS EXISTS FOR (ENC-1497) was not a wrong verdict -- it was two
    checkouts of the same commit producing DIFFERENT verdicts with nothing in
    the output to say why. `third_party/` is gitignored, so RapidJSON resolves
    to `<root>/third_party/rapidjson` in the shared checkout and to
    `/usr/include` in a fresh worktree, and neither run printed the path it had
    resolved. A gate whose verdict depends on the checkout must print the thing
    that differs.
    """
    if not path:
        return ("not reported by the manifest -- stale build directory; "
                "re-run cmake so the manifest carries rapidjson_include_dir")
    real = os.path.realpath(path)
    real_root = os.path.realpath(root)
    engine_inc = os.path.realpath(os.path.join(root, "include"))
    vendor = os.path.realpath(os.path.join(root, VENDOR_SUBDIR))
    if _under(real, engine_inc):
        return f"{path} (in-repo: the engine's own include/)"
    if _under(real, vendor) and _under(vendor, real_root):
        return (f"{path} (vendored under {VENDOR_SUBDIR}/ -- permitted by "
                f"check B, ENC-1497)")
    if _under(real, real_root):
        return (f"{path} (in-repo and NOT under {VENDOR_SUBDIR}/ -- check B "
                f"flags this)")
    return f"{path} (external to the repo)"


# A link entry is not always a bare target name, and MATCHING A DECORATED
# ENTRY AS IF IT WERE ONE IS A BYPASS, not a cosmetic problem.  Three forms are
# legal in LINK_LIBRARIES and none of them unwraps to a clean name:
#
#     $<LINK_ONLY:krn_data>                      one genex head
#     $<IF:$<BOOL:ON>,krn_data,>                 commas, nested head
#     $<LINK_LIBRARY:WHOLE_ARCHIVE,krn_data>     CMake >= 3.24, feature + comma
#     $<$<CONFIG:Debug>:krn_data>                head is itself a genex
#     krn::@(0x55f0)                             directory-scope marker on an
#                                                INTERFACE_LINK_LIBRARIES entry
#
# So this file does NOT rely on reducing an entry to one name.  It extracts
# every identifier-shaped TOKEN in the entry and pattern-matches all of them,
# and tries each as a graph key so the walk does not stop an edge short when
# the edge was wrapped.  Over-extraction is the safe direction: a spurious
# token can only cause a false FAILURE, which a human reads, while a missed one
# causes a false pass, which nobody does.
#
# PROVENANCE: the manifest DOES carry such a form, so this matching is
# load-bearing, not defensive.  `$<TARGET_PROPERTY:...>` inside
# `file(GENERATE)` evaluates most of a property's generator expressions -- the
# real `link.krn_engine` row is plain names -- but not `$<BUILD_INTERFACE:...>`.
# ENC-1561 measured `link.krn_host=krn_engine|$<BUILD_INTERFACE:krn_connector_market>`
# in a real manifest: a form that gets past install(EXPORT), configures and
# builds, and is caught only because the entry is decoded here (ENC-1585;
# selftest `D/build-interface-wrapped-market-link`).  The other forms above
# are not observed yet and their selftest cases say so.
GENEX_HEAD_RE = re.compile(r"^\$<(?:[A-Za-z_0-9]+|\$<[^<>]*>):(.*)>$")
DIRSCOPE_RE = re.compile(r"::@\(?[^)]*\)?$")
# `+`, `.` and `-` appear in real library names (`stdc++`, `libm.so`,
# `foo-bar`); `::` in namespaced targets.  Commas and genex punctuation are
# separators, which is what splits `WHOLE_ARCHIVE,krn_data` apart.
TOKEN_RE = re.compile(r"[A-Za-z0-9_.+:-]+")


def link_item_name(item: str) -> str:
    """The most likely bare target name, for recursion and for the report.

    Best effort only -- `link_item_candidates` is what the checks match on.
    """
    name = item.strip()
    while True:
        m = GENEX_HEAD_RE.match(name)
        if not m:
            break
        name = m.group(1).strip()
    name = DIRSCOPE_RE.sub("", name).strip()
    # A residue like `WHOLE_ARCHIVE,krn_data` or `$<BOOL:ON>,krn_data,` is not
    # a name; prefer its last identifier-shaped token, which is where CMake
    # puts the library in every genex form above.
    if "," in name or "<" in name or ">" in name:
        tokens = [t for t in TOKEN_RE.findall(name) if not t.startswith("$")]
        if tokens:
            name = DIRSCOPE_RE.sub("", tokens[-1])
    return name.strip()


def link_item_candidates(item: str) -> list[str]:
    """Every string in a link entry that could name a library.

    The unwrapped name plus every identifier-shaped token, deduped.  Checks C
    and D match patterns against all of these, so a forbidden name cannot hide
    inside a generator expression this parser does not fully understand.
    """
    out = [link_item_name(item)]
    for tok in TOKEN_RE.findall(item.strip()):
        tok = DIRSCOPE_RE.sub("", tok).strip()
        if tok and tok not in out:
            out.append(tok)
    return [c for c in out if c]


def parse_link_graph(manifest: dict[str, str]) -> dict[str, list[str]]:
    """target -> its link libraries, from the manifest's `link.<target>` rows.

    LINK_LIBRARIES and INTERFACE_LINK_LIBRARIES are merged per target: for the
    invariant it does not matter which of the two carries the forbidden entry,
    only that the link line ends up with it.
    """
    graph: dict[str, list[str]] = {}
    for key, value in manifest.items():
        for prefix in ("link.", "interface_link."):
            if key.startswith(prefix):
                graph.setdefault(key[len(prefix):], []).extend(split_list(value))
                break
    return {t: dedupe(libs) for t, libs in graph.items()}


def link_closure(graph: dict[str, list[str]],
                 root: str) -> list[tuple[str, list[str], tuple[str, ...]]]:
    """Every library reachable from `root`: (name, candidates, chain).

    Breadth-first so the reported chain is a shortest one, and cycle-safe.  A
    name with no row in the graph is a leaf (a system library, an imported
    target) and is matched but not expanded.  EVERY candidate of an entry is
    tried as a graph key, not just the best-guess name, so a wrapped edge --
    `$<LINK_LIBRARY:WHOLE_ARCHIVE,krn>` -- is still traversed instead of
    stopping the walk one edge short.
    """
    out: list[tuple[str, list[str], tuple[str, ...]]] = []
    reported: set[str] = {root}
    expanded: set[str] = {root}
    queue: list[tuple[str, tuple[str, ...]]] = [(root, (root,))]
    while queue:
        target, path = queue.pop(0)
        for raw in graph.get(target, ()):
            cands = link_item_candidates(raw)
            if not cands:
                continue
            name = cands[0]
            chain = path + (name,)
            if name not in reported:
                reported.add(name)
                out.append((name, cands, chain))
            for cand in cands:
                if cand in graph and cand not in expanded:
                    expanded.add(cand)
                    queue.append((cand, path + (cand,)))
    return out


# A name that belongs to this repo rather than to the system or a dependency.
# `krn`, `krn_engine`, `krn_data`, `krn::arrow`, `krn::data` all match.
FIRST_PARTY_RE = re.compile(r"^krn(?:[_:].*)?$")


def forbidden_links(graph: dict[str, list[str]], root: str) -> list[str]:
    """Checks C and D: nothing forbidden anywhere in `root`'s link closure.

    Transitive, not direct -- see the docstring.  The chain is printed because
    the fix is almost never on the root's own link line.

    TWO rules, and the second one is what closes the hole ONE LEVEL DEEPER
    than the review found it.  The manifest's target registry is written by
    hand in CMakeLists.txt, and nothing in CMake makes it complete, so a new
    first-party library -- `krn_store`, say -- would arrive with no row, be
    treated as a LEAF, and have its own dependencies never walked; an ALIAS
    (`add_library(krn::data ALIAS krn_data)`) does the same thing without even
    being a new target.  Either one makes the live-only variant unlinkable while
    rule one stays clean.  So a reached name that LOOKS first-party and has no
    row is itself a failure: not "this links something forbidden" but "this
    subtree was never searched, so a clean verdict about it is a verdict from
    a search that did not happen".
    """
    problems: list[str] = []
    for name, cands, chain in link_closure(graph, root):
        if any(MARKET_CONNECTOR_LINK.search(c) for c in cands):
            continue  # reported once, by market_reachers
        hit = next((c for c in cands
                    for p in FORBIDDEN_LINK_PATTERNS if p.search(c)), None)
        if hit is not None:
            shown = hit if hit == name else f"{name}' (as '{hit}"
            problems.append(
                f"{root} links '{shown}' via {' -> '.join(chain)} -- Arrow, "
                f"the ClickHouse client and krn_data must never be "
                f"reachable from {root}'s link line, because SPEC D14's "
                f"live-only client variant omits krn_data entirely and "
                f"must still link")
            continue
        if FIRST_PARTY_RE.match(name) and name not in graph:
            problems.append(
                f"{root} links '{name}' via {' -> '.join(chain)} -- that is a "
                f"first-party name with NO link.<target> row in the manifest, "
                f"so its own dependencies were never walked and this closure "
                f"is incomplete. Add it to _krn_iso_link_targets in "
                f"CMakeLists.txt (an ALIAS belongs there under its real "
                f"target's name), or if it is genuinely third-party, rename "
                f"it so it does not read as ours")
    return problems


def market_reachers(graph: dict[str, list[str]]) -> list[str]:
    """Every graphed target that reaches krn_connector_market.  (D11.1)

    Over EVERY row, like control_reachers, not only the checked roots: the
    market connector and both of its consumers left for krn-common (ENC-1570),
    so no target in this repo may link it, and a new test or bench binary
    that does is as much a violation as krn_engine doing it.
    """
    problems: list[str] = []
    for target in sorted(graph):
        if target == CONTROL_TARGET:
            continue
        for name, cands, chain in link_closure(graph, target):
            market = next((c for c in cands
                           if MARKET_CONNECTOR_LINK.search(c)), None)
            if market is not None:
                shown = market if market == name else f"{name}' (as '{market}"
                problems.append(
                    f"{target} links '{shown}' via {' -> '.join(chain)} -- the "
                    f"market connector is krn-common's (purity SPEC D11.1, "
                    f"ENC-1570) and no target in this repo may reach it")
                break
    return problems


def control_reachers(graph: dict[str, list[str]]) -> list[str]:
    """Every graphed target other than the control that reaches the control.

    The control is scanned, never linked.  Read off the real link graph, over
    EVERY row rather than only the checked roots, so a target the roots do not
    reach cannot quietly pick it up either.
    """
    problems: list[str] = []
    controls = (CONTROL_TARGET, CONTROL_PROBE, NM_CONTROL_TARGET)
    for target in sorted(graph):
        if target in (CONTROL_TARGET, NM_CONTROL_TARGET):
            continue
        for _name, cands, chain in link_closure(graph, target):
            hit = next((c for c in controls if c in cands), None)
            if hit is not None:
                problems.append(
                    f"{target} links the isolation control '{hit}' via "
                    f"{' -> '.join(chain)} -- {CONTROL_TARGET} and "
                    f"{NM_CONTROL_TARGET} exist only to be scanned and must "
                    f"never be linked into a real target")
                break
    return problems


# --- check E, part 1: the comment and literal stripper (ENC-1578) ----------
# Purity SPEC D11.3.  Check E is lexical, so prose that documents the
# invariant must not count as a use of what it documents.  strip_cpp blanks
# every comment and the body of every string/char literal, and nothing else.
#
# Output has the input's length and its newlines in the same places, so a
# line or column in the stripped text is the same line or column in the file.
# Blanked characters become spaces.  Quotes and literal prefixes stay
# (`R"`, `u8"`, `'`); the raw-string delimiter is blanked with the body.
#
# A header-name is not a string literal: in `#include "a/b.hpp"` and
# `__has_include("a/b.hpp")` the path is kept verbatim, `//` and all.
#
# Over-stripping is as bad as under-stripping: code mistaken for a comment or
# a literal is code check E never sees.  The selftest pins both directions.
ENCODING_PREFIXES = frozenset(("u8", "u", "U", "L"))
RAW_PREFIXES = frozenset(("R", "u8R", "uR", "UR", "LR"))
INCLUDE_DIRECTIVES = frozenset(("include", "include_next", "import"))
HAS_INCLUDE = frozenset(("__has_include", "__has_include_next"))
RAW_DELIM_MAX = 16


def _ident_char(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


def strip_cpp(text: str) -> str:
    """`text` with comments and literal bodies blanked.  See the block above."""
    n = len(text)
    out = list(text)

    def skip(i: int) -> int:
        # Past any line splices (phase 2) at i.
        while True:
            if text.startswith("\\\n", i):
                i += 2
            elif text.startswith("\\\r\n", i):
                i += 3
            else:
                return i

    def blank(a: int, b: int) -> None:
        for k in range(a, min(b, n)):
            if text[k] not in "\r\n":
                out[k] = " "

    def literal(q: int) -> int:
        # text[q] is the opening quote of an ordinary string/char literal.
        quote = text[q]
        m = q + 1
        while m < n:
            ch = text[m]
            if ch == "\\":
                m += 3 if text.startswith("\\\r\n", m) else 2
                continue
            if ch == quote or ch == "\n":
                break
            m += 1
        m = min(m, n)
        blank(q + 1, m)
        return m + 1 if m < n and text[m] == quote else m

    def raw(q: int) -> int:
        # text[q] is the `"` of a raw string.  Splices are reverted inside a
        # raw string ([lex.pptoken]), so this reads the physical text.
        p = q + 1
        while (p < n and p - q - 1 <= RAW_DELIM_MAX
               and text[p] not in '() \\\t\v\f\r\n"'):
            p += 1
        if p >= n or text[p] != "(" or p - q - 1 > RAW_DELIM_MAX:
            return literal(q)  # ill-formed; lex it as an ordinary string
        close = ")" + text[q + 1:p] + '"'
        e = text.find(close, p + 1)
        end = n if e < 0 else e + len(close) - 1
        blank(q + 1, end)
        return min(end + 1, n)

    # Where we are in a directive, for header-names:
    # line -> `#` -> hash -> `include` -> include;  `__has_include` -> has
    # -> `(` -> has_paren.  Whitespace and comments leave it unchanged.
    state = "line"
    i = skip(0)
    while i < n:
        c = text[i]
        j = skip(i + 1)
        c2 = text[j] if j < n else ""
        if c == "/" and c2 == "/":
            k = j + 1
            while True:
                k = skip(k)
                if k >= n or text[k] == "\n":
                    break
                k += 1
            blank(i, k)
            i = k
            continue
        if c == "/" and c2 == "*":
            k = skip(j + 1)
            end = n
            while k < n:
                k2 = skip(k + 1)
                if text[k] == "*" and k2 < n and text[k2] == "/":
                    end = k2 + 1
                    break
                k = k2
            blank(i, end)
            i = skip(end)
            continue
        if c == "\n":
            state = "line"
            i = j
            continue
        if c in " \t\r\v\f":
            i = j
            continue
        if c in '"<' and state in ("include", "has_paren"):
            closing = '"' if c == '"' else ">"
            k = i + 1
            while k < n and text[k] not in (closing, "\n"):
                k += 1
            state = "none"
            i = skip(k + 1) if k < n and text[k] == closing else k
            continue
        if c.isalpha() or c == "_":
            k, word = i, []
            while k < n and _ident_char(text[k]):
                word.append(text[k])
                k = skip(k + 1)
            w = "".join(word)
            if k < n and text[k] == '"' and w in RAW_PREFIXES:
                state, i = "none", skip(raw(k))
            elif k < n and text[k] in "\"'" and w in ENCODING_PREFIXES:
                state, i = "none", skip(literal(k))
            else:
                if state == "hash" and w in INCLUDE_DIRECTIVES:
                    state = "include"
                elif w in HAS_INCLUDE:
                    state = "has"
                else:
                    state = "none"
                i = k
            continue
        if c.isdigit() or (c == "." and c2.isdigit()):
            # A pp-number, so `1'000` and `0x1'F` keep their `'`.
            k, prev = i, ""
            while k < n:
                ch = text[k]
                if _ident_char(ch) or ch == ".":
                    prev, k = ch, skip(k + 1)
                elif ch in "+-" and prev in ("e", "E", "p", "P"):
                    prev, k = ch, skip(k + 1)
                elif ch == "'":
                    k2 = skip(k + 1)
                    if k2 < n and _ident_char(text[k2]):
                        prev, k = "'", k2
                    else:
                        break
                else:
                    break
            state, i = "none", k
            continue
        if c in "\"'":
            state, i = "none", skip(literal(i))
            continue
        if c == "#":
            state = "hash" if state == "line" else "none"
        elif c == "(":
            state = "has_paren" if state == "has" else "none"
        else:
            state = "none"
        i = j
    return "".join(out)


def include_paths(text: str) -> list[tuple[int, str]]:
    """(line number, path) of every line INCLUDE_RE matches."""
    return [(no, m.group(1))
            for no, line in enumerate(text.split("\n"), start=1)
            for m in (INCLUDE_RE.match(line),) if m]


def strip_defects(text: str, stripped: str) -> list[str]:
    """What is wrong with `stripped` as the stripping of `text`.

    Shape: same length, newlines in the same places.  Includes: every
    `#include` path that survives is the one in the file, on the same line,
    and none is invented.  One that vanished was inside a comment.
    """
    if len(stripped) != len(text):
        return [f"length {len(text)} became {len(stripped)}"]
    bad = [k for k, (a, b) in enumerate(zip(text, stripped))
           if (a in "\r\n" or b in "\r\n") and a != b]
    if bad:
        return [f"a line break moved at offset {bad[0]}"]
    raw_inc = dict(include_paths(text))
    return [f"line {no}: #include path {path!r} is not the file's "
            f"({raw_inc.get(no)!r})"
            for no, path in include_paths(stripped)
            if raw_inc.get(no) != path]


# The comment mentions purity SPEC D11.3 counts (seven at b4c67e8).  REPORTED,
# not gated: check E's gated vocabulary is derived, not this.
D11_SENTINEL_RE = re.compile(r"\bMarket(?:TickComputer|FieldMap)\b")


def strip_sources(sources: list[str]) -> tuple[dict[str, str], dict, list[str]]:
    """Strip each file of check A's list: (stripped, stats, defects)."""
    stripped: dict[str, str] = {}
    stats = {"files": 0, "blanked": 0, "includes": 0, "includes_in_comments": 0,
             "sentinel_before": 0, "sentinel_after": 0}
    defects: list[str] = []
    for path in sources:
        try:
            with open(path, "r", encoding="utf-8", errors="replace",
                      newline="") as fh:
                text = fh.read()
        except OSError as exc:
            defects.append(f"{path}: cannot read ({exc})")
            continue
        s = strip_cpp(text)
        stripped[path] = s
        stats["files"] += 1
        stats["blanked"] += sum(1 for a, b in zip(text, s) if a != b)
        kept, raw_inc = include_paths(s), include_paths(text)
        stats["includes"] += len(kept)
        stats["includes_in_comments"] += len(raw_inc) - len(kept)
        stats["sentinel_before"] += len(D11_SENTINEL_RE.findall(text))
        stats["sentinel_after"] += len(D11_SENTINEL_RE.findall(s))
        defects += [f"{path}: {d}" for d in strip_defects(text, s)]
    return stripped, stats, defects


# --- check E, part 2: the derived vocabulary (ENC-1579) --------------------
# tools/market_vocabulary.txt is generated by tools/derive_market_vocabulary.py
# from the market code in git history; the selftest regenerates it and fails
# on any difference, so a hand edit is caught.  Matched per identifier token
# of the STRIPPED text, so a comment or a literal never counts.
VOCAB_FILE = os.path.join("tools", "market_vocabulary.txt")
VOCAB_FIXTURE = os.path.join("tools", "fixtures", "check_e_market_vocabulary.cpp")
BUILTINS_FILE = os.path.join("src", "core", "BuiltinFunctions.cpp")
# The vocabulary's five source tags.  The fixture must hit every one.
VOCAB_SOURCES = ("atomic", "fieldmap", "k_literal", "obkey", "type")
# 119 words at the derivation SHAs; a list this short is truncated.
MIN_VOCAB_WORDS = 80
# The fixture names ten derived words in code, two per source tag, so ten is
# the count at which every one of them hit.  A lost word or a dead source
# drops below it.
MIN_FIXTURE_HITS = 10
FIXTURE_EXEMPT_WORD = "mean"
IDENT_TOKEN_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*")

# word -> (class, reason).  "builtin" is verified: BuiltinFunctions.cpp must
# register the word, or the exemption is stale.  "word" is a recorded
# judgement.  Every key must be in the vocabulary, or it is stale.
VOCAB_EXEMPT = {
    "spread": ("builtin", "max(v) - min(v) over a vector<double>"),
    "last": ("builtin", "v.back()"),
    "mean": ("builtin", "arithmetic mean; also a local in its own builtin"),
    "median": ("builtin", "middle of the sorted vector"),
    "min": ("builtin", "parseObKey reduction; std::min in engine code"),
    "max": ("builtin", "parseObKey reduction; std::max in engine code"),
    "count": ("builtin", "parseObKey reduction; .count() in engine code"),
    "sum": ("builtin", "parseObKey reduction"),
    "avg": ("builtin", "parseObKey reduction"),
    "range": ("builtin", "parseObKey range.* token; max - min"),
    "size": ("word", "SPEC D11.3 names it ordinary; .size() in engine code"),
    "name": ("word", "MarketFieldMap's profile key; every engine registry "
                     "names its entries"),
    "level": ("word", "parseObKey's level.* token; the Logger's log level"),
    "agg": ("word", "parseObKey's .agg suffix; TreeBuilder's Aggregate local"),
    "default": ("word", "MarketFieldMap's default profile name; a C++ "
                        "keyword, never an identifier"),
}


def read_vocabulary(path: str) -> dict[str, list[str]]:
    """word -> source tags, from the generated file."""
    out: dict[str, list[str]] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            word, _, tags = line.partition("\t")
            out[word] = tags.split(",") if tags else []
    return out


def vocab_matcher(vocab: dict[str, list[str]]):
    """A function token -> vocabulary word or None.  `sma_` is a prefix."""
    exact = {w for w in vocab if not w.endswith("_")}
    prefixes = tuple(w for w in vocab if w.endswith("_"))

    def match(tok: str):
        if tok in exact:
            return tok
        return next((p for p in prefixes if tok.startswith(p)), None)
    return match


def vocab_hits(stripped: str, vocab: dict[str, list[str]], match=None
               ) -> list[tuple[int, str, str]]:
    """(line, token, vocabulary word) for each identifier in `stripped`."""
    match = match or vocab_matcher(vocab)
    out = []
    for m in IDENT_TOKEN_RE.finditer(stripped):
        word = match(m.group(0))
        if word is not None:
            out.append((stripped.count("\n", 0, m.start()) + 1, m.group(0),
                        word))
    return out


def exemption_defects(root: str, vocab: dict[str, list[str]]) -> list[str]:
    """Stale exemptions: not in the vocabulary, or a builtin nobody registers."""
    out = []
    try:
        with open(os.path.join(root, BUILTINS_FILE), encoding="utf-8") as fh:
            builtins = set(re.findall(r'registerFunction\("(\w+)"', fh.read()))
    except OSError:
        builtins = set()
    for word, (kind, _reason) in sorted(VOCAB_EXEMPT.items()):
        if word not in vocab:
            out.append(f"exemption {word!r} is not in the vocabulary")
        elif kind == "builtin" and word not in builtins:
            out.append(f"exemption {word!r} claims to be a builtin, but "
                       f"{BUILTINS_FILE} does not register it")
    return out


def check_e(root: str, stripped: dict[str, str]
            ) -> tuple[list[str], list[str], dict]:
    """(vacuous, problems, stats): the vocabulary over check A's stripped list,
    and the fixture control, which is scanned on every run."""
    vacuous: list[str] = []
    problems: list[str] = []
    stats = {"words": 0, "exempt_hits": {}, "fixture": "not run"}
    try:
        vocab = read_vocabulary(os.path.join(root, VOCAB_FILE))
    except OSError as exc:
        return ([f"check E has no vocabulary: {exc}"], [], stats)
    stats["words"] = len(vocab)
    if len(vocab) < MIN_VOCAB_WORDS:
        vacuous.append(f"check E's vocabulary has {len(vocab)} words (floor "
                       f"{MIN_VOCAB_WORDS}) -- {VOCAB_FILE} is truncated")
    vacuous += [f"check E: {d}" for d in exemption_defects(root, vocab)]
    match = vocab_matcher({w: t for w, t in vocab.items()
                           if w not in VOCAB_EXEMPT})
    exempt = vocab_matcher({w: vocab.get(w, []) for w in VOCAB_EXEMPT})

    for path in sorted(stripped):
        rel = os.path.relpath(path, root)
        for line, tok, word in vocab_hits(stripped[path], vocab, match):
            problems.append(
                f"{rel}:{line}: engine code names '{tok}', market vocabulary "
                f"({word}: {','.join(vocab[word])}) -- check E, purity SPEC "
                f"D11.3; the engine names no market-data type")
        for _line, _tok, word in vocab_hits(stripped[path], vocab, exempt):
            stats["exempt_hits"][word] = stats["exempt_hits"].get(word, 0) + 1

    # The positive control.
    fx = os.path.join(root, VOCAB_FIXTURE)
    try:
        with open(fx, encoding="utf-8", newline="") as fh:
            raw = fh.read()
    except OSError as exc:
        vacuous.append(f"check E's positive control is missing ({exc}) -- "
                       f"nothing shows the vocabulary can match anything")
        stats["fixture"] = "MISSING"
        return vacuous, problems, stats
    lines = raw.split("\n")
    fhits = vocab_hits(strip_cpp(raw), vocab, match)
    tags = {t for _, _, w in fhits for t in vocab[w]}
    leaks = [f"{tok}@{ln}" for ln, tok, _ in fhits
             if "DECOY" in lines[ln - 1]]
    exempt_hit = [tok for _, tok, _ in fhits if tok == FIXTURE_EXEMPT_WORD]
    bad = []
    if len(fhits) < MIN_FIXTURE_HITS:
        bad.append(f"{len(fhits)} hits (floor {MIN_FIXTURE_HITS})")
    if set(VOCAB_SOURCES) - tags:
        bad.append(f"no hit from source(s) {sorted(set(VOCAB_SOURCES) - tags)}")
    if leaks:
        bad.append(f"comment/literal words counted: {leaks}")
    if exempt_hit or FIXTURE_EXEMPT_WORD not in VOCAB_EXEMPT:
        bad.append(f"exempt word {FIXTURE_EXEMPT_WORD!r} was not exempted")
    if bad:
        vacuous.append(f"check E's positive control {VOCAB_FIXTURE} failed: "
                       f"{'; '.join(bad)}")
        stats["fixture"] = f"FAILED ({'; '.join(bad)})"
    else:
        stats["fixture"] = (f"ok ({len(fhits)} hits >= {MIN_FIXTURE_HITS}, "
                            f"{len(tags)}/{len(VOCAB_SOURCES)} sources, "
                            f"0 decoy leaks, exempt word not counted)")
    return vacuous, problems, stats


def selftest_vocabulary(failures: list[str], log: list[str]) -> int:
    """Regenerate the vocabulary; prove a hand edit and check E's verdicts."""
    import importlib.util

    ran = 0
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    spec = importlib.util.spec_from_file_location(
        "derive_market_vocabulary",
        os.path.join(here, "derive_market_vocabulary.py"))
    derive = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(derive)
    try:
        regenerated = derive.render(derive.derive())
    except derive.DeriveError as exc:
        failures.append(f"SELFTEST: E/regenerate: cannot derive ({exc})")
        return ran
    why = derive.drift(os.path.join(root, VOCAB_FILE), regenerated)
    if why:
        failures.append(f"SELFTEST: E/regenerate: {why} -- hand-edited or "
                        f"stale; run tools/derive_market_vocabulary.py --write")
    log.append(f"  E/regenerate: {VOCAB_FILE} matches its derivation "
               f"({why is None})")
    ran += 1

    # A hand edit: drop one word from a copy.  The same drift() must see it.
    import tempfile
    with open(os.path.join(root, VOCAB_FILE), encoding="utf-8") as fh:
        committed = fh.read()
    edited = "".join(ln for ln in committed.splitlines(keepends=True)
                     if not ln.startswith("MarketTickComputer\t"))
    with tempfile.TemporaryDirectory(prefix="enc1579-vocab-") as tmp:
        copy = os.path.join(tmp, "market_vocabulary.txt")
        with open(copy, "w", encoding="utf-8") as fh:
            fh.write(edited)
        caught = derive.drift(copy, regenerated)
    if edited == committed:
        failures.append("SELFTEST: E/hand-edit: mutation did not apply")
    elif not caught or "MarketTickComputer" not in caught:
        failures.append(f"SELFTEST: E/hand-edit: dropping a word was not "
                        f"detected ({caught!r})")
    log.append(f"  E/hand-edit: dropping MarketTickComputer applied "
               f"({len(committed)} -> {len(edited)} chars); drift says: "
               f"{caught}")
    ran += 1

    vocab = read_vocabulary(os.path.join(root, VOCAB_FILE))
    match = vocab_matcher({w: t for w, t in vocab.items()
                           if w not in VOCAB_EXEMPT})
    # (name, text, hits expected)
    cases = (
        ("identifier", "MarketTickComputer c;\n", 1),
        ("comment", "// MarketTickComputer c;\n", 0),
        ("block-comment", "/* MarketTickComputer */ int c;\n", 0),
        ("string", 'auto s = "MarketTickComputer";\n', 0),
        ("raw-string", 'auto s = R"(MarketTickComputer)";\n', 0),
        ("prefix", "double sma_20;\n", 1),
        ("prefix-not-infix", "double my_sma_20;\n", 0),
        ("substring-not-word", "int OrderBookish;\n", 0),
        ("exempt-builtin", "double mean, spread, last, median;\n", 0),
        ("not-exempt", "double vwap;\n", 1),
    )
    for name, text, want in cases:
        got = len(vocab_hits(strip_cpp(text), vocab, match))
        log.append(f"  E/vocab-{name}: {got} hit(s), want {want}")
        if got != want:
            failures.append(f"SELFTEST: E/vocab-{name}: {got} hits in "
                            f"{text!r}, expected {want}")
        ran += 1
    for word in ("spread", "last", "mean", "median"):
        if VOCAB_EXEMPT.get(word, ("",))[0] != "builtin":
            failures.append(f"SELFTEST: E/exempt-{word}: not a recorded "
                            f"builtin exemption")
    ran += 1
    # Stale exemptions are refused.
    stale = exemption_defects(root, {w: t for w, t in vocab.items()
                                     if w != "spread"})
    if not any("'spread'" in d for d in stale):
        failures.append("SELFTEST: E/stale-exemption: an exemption missing "
                        "from the vocabulary was not refused")
    ran += 1
    return ran


def selftest_stripper(failures: list[str], log: list[str]) -> int:
    """Check E's stripper, both directions, with mutants that prove they applied.

    A MUTANT is (base text, substring, replacement, sentinel).  It first
    asserts the substitution changed the text and logs that; only then does it
    judge the verdict, which is whether the sentinel survives stripping.  The
    verdict must DIFFER between base and mutant, so a mutant whose base
    already gives the mutant's answer fails too.  A no-op substitution fails
    the selftest: that is asserted below on a deliberately no-op mutant.
    """
    ran = 0

    def survives(text: str, word: str) -> bool:
        return re.search(rf"\b{word}\b", strip_cpp(text)) is not None

    def check_shape(name: str, text: str) -> None:
        for d in strip_defects(text, strip_cpp(text)):
            failures.append(f"SELFTEST: E/{name}: {d}")

    def mutant(name, base, old, new, word, base_survives, sink=failures,
               out=log):
        mutated = base.replace(old, new, 1)
        if old not in base or mutated == base:
            sink.append(f"SELFTEST: E/{name}: substitution {old!r} -> {new!r} "
                        f"did not change the text; its verdict proves nothing")
            out.append(f"  E/{name}: substitution NOT applied -- FAIL")
            return
        first = next((k for k, (a, b) in enumerate(zip(base, mutated))
                      if a != b), min(len(base), len(mutated)))
        out.append(f"  E/{name}: substitution applied (text differs from "
                   f"offset {first}, {len(base)} -> {len(mutated)} chars); {word} {'survives' if base_survives else 'is stripped'}"
                   f" in base, {'is stripped' if base_survives else 'survives'}"
                   f" in mutant")
        for label, text, want in (("base", base, base_survives),
                                  ("mutant", mutated, not base_survives)):
            check_shape(f"{name}/{label}", text)
            if survives(text, word) != want:
                sink.append(f"SELFTEST: E/{name}: {word} "
                            f"{'was stripped' if want else 'survived'} in the "
                            f"{label} {text!r} -> {strip_cpp(text)!r}")

    # (name, base, substring, replacement, sentinel, sentinel survives in base)
    # Each moves the sentinel across a comment or literal boundary.
    cases = (
        ("line-comment", "int a; // Hit\n", "// Hit", "Hit //", "Hit", False),
        ("block-comment", "int a; /* Hit */ int b;\n", "/* Hit */",
         "/* */ Hit", "Hit", False),
        ("string", 'auto s = "Hit";\n', '"Hit"', 'Hit ""', "Hit", False),
        ("char", "auto c = 'Hit';\n", "'Hit'", "Hit", "Hit", False),
        ("raw-string", 'auto s = R"(Hit)";\n', 'R"(Hit)"', 'R"()" Hit',
         "Hit", False),
        ("raw-string-delimiter", 'auto s = R"x(a)" Hit )x"; int b;\n',
         'R"x(', 'R"(', "Hit", False),
        ("u8R-raw-string", 'auto s = u8R"(// Hit)";\n', 'u8R"(// Hit)"',
         'u8R"(//)" Hit', "Hit", False),
        ("LR-raw-string", 'auto s = LR"(Hit)";\n', 'LR"(Hit)"', 'LR"()" Hit',
         "Hit", False),
        ("encoding-prefix-string", 'auto s = u8"Hit";\n', 'u8"Hit"',
         'u8"" Hit', "Hit", False),
        ("line-continuation", "int a; // x \\\nHit;\n", "\\\n", "\n",
         "Hit", False),
        ("spliced-comment-opener", "int a; /\\\n/ Hit\n", "/\\\n/", "/ \n/",
         "Hit", False),
        ("nested-looking-block", "/* /* */ Keep; */\n", "/* /* */", "/* /* ",
         "Keep", True),
        # The traps: real code beside a comment or literal must survive.
        ("slashes-in-string", 'auto u = "http://x"; Keep;\n', '"http://x"',
         "http://x", "Keep", True),
        ("slashes-in-raw-string", 'std::regex r(R"(^(http)://)"); Keep;\n',
         'R"(^(http)://)"', "R//", "Keep", True),
        ("quote-in-comment", '/* " */ Keep; // "\n', '/* " */', '" ',
         "Keep", True),
        ("quote-in-line-comment", '// "\nKeep;\n', '// "\n', '"',
         "Keep", True),
        ("dquote-in-char", "char c = '\"'; Keep; char d = '\"';\n", "'\"'",
         "'", "Keep", True),
        ("escaped-quote-in-string", 'auto s = "a\\"b"; Keep;\n', '\\"', '"',
         "Keep", True),
        ("escaped-backslash", 'auto s = "a\\\\"; Keep; "b";\n', '\\\\"', '\\"',
         "Keep", True),
        ("escaped-squote", "char c = '\\''; Keep; char d = 'x';\n", "\\'", "'",
         "Keep", True),
        ("digit-separator", "int n = 1'000; Keep; char c = 'x';\n", "1'000",
         "'000", "Keep", True),
        ("hex-digit-separator", "int n = 0x1'F; Keep; char c = 'x';\n",
         "0x1'F", "x'1F", "Keep", True),
        ("division-is-not-a-comment", "int q = a / b; Keep;\n", "a / b",
         "a //b", "Keep", True),
        ("line-comment-ends-at-newline", "// note\nKeep;\n", "\n", " ",
         "Keep", True),
        ("block-comment-ends", "/* a */ Keep;\n", "*/", "*", "Keep", True),
        ("raw-string-ends-at-its-delimiter",
         'auto s = R"x(a)" b)x"; Keep;\n', ')x";', ')y";', "Keep", True),
        ("identifier-is-not-a-prefix", 'auto s = MyR"(a"; Keep; ")";\n',
         "MyR", "R", "Keep", True),
    )
    for case in cases:
        mutant(*case)
        ran += 1

    # #include paths are header-names, not literals: kept verbatim, even with
    # `//` in them, and stripped only when the directive is in a comment.
    inc = '#include "krn/a//b.hpp"\n#  include <krn/c.hpp> // tail\n'
    s = strip_cpp(inc)
    got = [p for _, p in include_paths(s)]
    log.append(f"  E/include-paths-kept: {got}")
    if got != ["krn/a//b.hpp", "krn/c.hpp"]:
        failures.append(f"SELFTEST: E/include-paths-kept: {got!r} from {s!r}")
    if "tail" in s:
        failures.append("SELFTEST: E/include-paths-kept: the trailing comment "
                        "survived")
    mutant("include-path-is-not-a-string", '#include "krn/Keep.hpp"\n',
           "#include", "auto x =", "Keep", True)
    mutant("has-include-path", '#if __has_include("krn/Keep.hpp")\n',
           "__has_include", "__has_includes", "Keep", True)
    mutant("include-in-block-comment", '/*\n#include "krn/Keep.hpp"\n*/\n',
           "/*\n", "\n", "Keep", False)
    ran += 4
    check_shape("crlf-and-splices", "a; // x\r\nb \\\r\nc; /* \\\r\n */ d;\r\n")
    ran += 1

    # The harness itself: a mutant whose substitution changes nothing must
    # fail, whatever the stripper does.  Run into a private sink.
    sink: list[str] = []
    noop_log: list[str] = []
    mutant("deliberate-no-op", "int a; // Hit\n", "// Hit", "// Hit", "Hit",
           False, sink=sink, out=noop_log)
    mutant("deliberate-absent", "int a; // Hit\n", "/* Hit */", "Hit", "Hit",
           False, sink=sink, out=noop_log)
    # Refused FOR BEING A NO-OP.  The verdict-must-flip rule also rejects a
    # no-op, so a bare count would pass with the applied check deleted.
    refused = [f for f in sink if "did not change the text" in f]
    if len(refused) != 2 or len(sink) != 2:
        failures.append(f"SELFTEST: E/no-op-mutant-is-rejected: a no-op "
                        f"substitution was not refused as one: {sink}")
    log.append(f"  E/no-op-mutant-is-rejected: {len(refused)} of 2 no-op "
               f"mutants refused as no-ops")
    ran += 1
    return ran


def selftest_gate(failures: list[str]) -> int:
    """The whole gate, exit code included, on a fixture manifest.  (ENC-1507)

    The fixture clears every anti-vacuity floor, and the legal case is asserted
    to exit 0 first, so each refusal below is caused by its own mutation and
    not by the fixture being thin.  Each mutant also asserts it changed the
    manifest before its verdict is judged.  The fixture has the repo's shape
    after ENC-1570: no connectors/, no src/main.cpp, no market target.
    """
    import shutil
    import tempfile

    ran: list[str] = []
    with tempfile.TemporaryDirectory(prefix="enc1507-gate-") as fx:
        os.makedirs(os.path.join(fx, "include", "krn"))
        os.makedirs(os.path.join(fx, "src", "core"))
        os.makedirs(os.path.join(fx, "src", "host"))
        os.makedirs(os.path.join(fx, APPS_DIR, "engine_server"))
        os.makedirs(os.path.join(fx, "plugins", "reference", "include", "krn",
                                 "reference"))
        app_main = os.path.join(fx, APPS_DIR, "engine_server", "main.cpp")
        with open(app_main, "w", encoding="utf-8") as fh:
            fh.write("#include <memory>\n")
        host_cpp = os.path.join(fx, "src", "host", "Host.cpp")
        with open(host_cpp, "w", encoding="utf-8") as fh:
            fh.write("#include <memory>\n")
        sources = []
        for i in range(MIN_ENGINE_FILES + 5):
            path = os.path.join(fx, "src", "core", f"E{i}.cpp")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(f"int e{i}; // fixture\n")
            sources.append(path)
        inc = os.path.join(fx, "include")
        # Check E's inputs, the real ones: the vocabulary and the fixture,
        # plus a BuiltinFunctions.cpp registering the builtin exemptions.
        here = os.path.dirname(os.path.abspath(__file__))
        os.makedirs(os.path.join(fx, "tools", "fixtures"))
        for rel in (VOCAB_FILE, VOCAB_FIXTURE):
            shutil.copy(os.path.join(os.path.dirname(here), rel),
                        os.path.join(fx, rel))
        with open(os.path.join(fx, BUILTINS_FILE), "w", encoding="utf-8") as fh:
            for w, (kind, _) in sorted(VOCAB_EXEMPT.items()):
                if kind == "builtin":
                    fh.write(f'void r{len(w)}() {{ registerFunction("{w}"); }}\n')

        base_graph = {
            "krn_engine": ["Threads::Threads"],
            "krn": ["krn_engine"],
            "krn_plugin_reference": ["krn_engine"],
            "krn_host": ["krn_engine"],
            "krn_engine_server": ["krn_host", "krn_plugin_reference"],
            "krn_tests": ["krn_engine", "krn_plugin_reference",
                          "GTest::gtest_main"],
            CONTROL_TARGET: [CONTROL_PROBE],
        }

        def manifest(graph: dict[str, list[str]]) -> dict[str, str]:
            m = {"root": fx, "engine_include_dirs": inc,
                 "engine_interface_include_dirs": inc,
                 "engine_cmake_sources": "|".join(sources),
                 "host_cmake_sources": host_cpp,
                 "engine_server_cmake_sources": app_main,
                 "rapidjson_include_dir": "/usr/include"}
            for t, libs in graph.items():
                m[f"link.{t}"] = "|".join(libs)
            return m

        def gate(name, graph, want, vacuity_mentions=None):
            # Through the file, so read_manifest is exercised too.
            path = os.path.join(fx, f"{len(os.listdir(fx))}.manifest")
            with open(path, "w", encoding="utf-8") as fh:
                for k, v in manifest(graph).items():
                    fh.write(f"{k}={v}\n")
            vac, probs, summary = evaluate(read_manifest(path))
            got = exit_code(vac, probs)
            ran.append(f"{name}={got}")
            if got != want:
                failures.append(
                    f"SELFTEST: gate/{name} exited {got}, expected {want} "
                    f"(vacuous={vac}, problems={probs})")
            if vacuity_mentions and not any(vacuity_mentions in v for v in vac):
                failures.append(
                    f"SELFTEST: gate/{name} did not refuse for the control: "
                    f"{vac}")
            return summary

        def mutant(name, edit):
            g = {t: list(libs) for t, libs in base_graph.items()}
            edit(g)
            if g == base_graph:
                failures.append(f"SELFTEST: gate/{name}: mutation did not apply")
            return g

        def append_line(label, path, line):
            with open(path, encoding="utf-8") as fh:
                before = fh.read()
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line)
            with open(path, encoding="utf-8") as fh:
                if fh.read() == before:
                    failures.append(f"SELFTEST: gate/{label}: mutation did "
                                    f"not apply")
            return before

        # The legal fixture passes, and its control fires FROM THE STUB -- the
        # only control since data/ left (ENC-1573).
        summary = gate("legal", base_graph, 0)
        if not any(f"ok ({CONTROL_TARGET} -> {CONTROL_PROBE}" in ln
                   for ln in summary):
            failures.append("SELFTEST: gate/legal: positive control "
                            "is not reported as ok from the stub")
        expect = forbidden_links(base_graph, CONTROL_TARGET)
        if not expect:
            failures.append("SELFTEST: the control stub does not fire")

        # ENC-1570: the apps/ rule's connector-root list is EMPTY here, which
        # is intended: a pass (0), not "could not run" (2), and the summary
        # says so.  The prefix rule still bites, below.
        if app_foreign_roots(fx, non_engine_header_roots(fx)) != []:
            failures.append("SELFTEST: gate/apps-rule-empty: the fixture has "
                            "a connector root, so this proves nothing")
        if not any("apps/ rule" in ln and "empty is intended" in ln
                   for ln in summary):
            failures.append("SELFTEST: gate/apps-rule-empty: the summary does "
                            f"not name the empty apps/ list: {summary}")
        ran.append("apps-rule-empty-is-intended=0")

        # An Arrow-linking target no checked root reaches is not a violation.
        with_data = mutant("legal-unreached-krn_data", lambda g: g.update(
            {"krn_data": ["krn_engine", "krn::arrow"]}))
        gate("legal-unreached-krn_data", with_data, 0)

        # No control -> must not exit 0.
        no_ctl = mutant("no-control", lambda g: g.pop(CONTROL_TARGET))
        gate("no-control", no_ctl, 2, CONTROL_TARGET)
        # Another target that trips a pattern is not a substitute.
        data_no_ctl = dict(no_ctl, krn_data=["krn_engine", "krn::arrow"])
        gate("krn_data-only", data_no_ctl, 2, CONTROL_TARGET)
        # A control row that matches nothing is no control.
        silent = mutant("control-silent", lambda g: g.update(
            {CONTROL_TARGET: ["Threads::Threads"]}))
        gate("control-silent", silent, 2, CONTROL_TARGET)
        # One that fires but not through the stub has drifted from CMake.
        drifted = mutant("control-without-stub", lambda g: g.update(
            {CONTROL_TARGET: ["krn::arrow"]}))
        gate("control-without-stub", drifted, 2, CONTROL_TARGET)

        # D11.1 after ENC-1570: krn_connector_market exists nowhere in this
        # repo, so a link to the NAME (an IMPORTED stub, or the target
        # re-added) is the violation -- from every checked root, directly
        # and through the umbrella, and from a target no root reaches.
        for r in ("krn_engine", "krn_host", "krn_engine_server", "krn_tests"):
            name = f"{r}-links-market-direct"
            gate(name, mutant(name, lambda g, r=r:
                              g[r].append("krn_connector_market")), 1)
        def via_umbrella(g, r):
            g["krn"].append("krn_connector_market")
            g[r].append("krn")
        for r in ("krn_host", "krn_engine_server", "krn_tests"):
            name = f"{r}-links-market-via-umbrella"
            gate(name, mutant(name, lambda g, r=r: via_umbrella(g, r)), 1)
        gate("umbrella-links-market",
             mutant("umbrella-links-market",
                    lambda g: g["krn"].append("krn_connector_market")), 1)
        gate("unreached-bench-links-market",
             mutant("unreached-bench-links-market", lambda g: g.update(
                 {"bench_x": ["krn", "krn_connector_market"]})), 1)
        gate("krn_server-re-added",
             mutant("krn_server-re-added", lambda g: g.update(
                 {"krn_server": ["krn_host", "krn_connector_market"]})), 1)

        # D11.2: a missing root is "could not run", never a pass.  krn_host
        # is removed with its edges, else the unregistered-name rule fires.
        def drop(g, r):
            g.pop(r)
            for t in g:
                g[t] = ["krn_engine" if x == r else x for x in g[t]]
        for r in ("krn_host", "krn_engine_server"):
            gate(f"no-{r}-row", mutant(f"no-{r}-row",
                                       lambda g, r=r: drop(g, r)), 2, r)

        # The apps/ prefix rule with the EMPTY connector-root list: an Arrow
        # include from krn_engine_server's source is still caught.
        before = append_line("apps-main-includes-arrow", app_main,
                             "#include <arrow/api.h>\n")
        gate("apps-main-includes-arrow", base_graph, 1)
        with open(app_main, "w", encoding="utf-8") as fh:
            fh.write(before)

        # A connector re-added under connectors/ is enumerated, so including
        # it from apps/ or krn_host is caught with no edit to this file.
        mkt = os.path.join(fx, "connectors", "market", "include", "krn",
                           "market")
        os.makedirs(mkt)
        open(os.path.join(mkt, "MarketConnector.hpp"), "w").close()
        if len(app_foreign_roots(fx, non_engine_header_roots(fx))) != 1:
            failures.append("SELFTEST: gate/connector-re-added: the stub "
                            "connector root was not enumerated")
        for label, path in (("apps-main", app_main), ("host-cpp", host_cpp)):
            before = append_line(f"{label}-includes-market", path,
                                 '#include "krn/market/MarketConnector.hpp"\n')
            gate(f"{label}-includes-market", base_graph, 1)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(before)
        shutil.rmtree(os.path.join(fx, "connectors"))
        gate("legal-after-include-restore", base_graph, 0)

        # ENC-1578: a stripper that blanks nothing over check A's list is
        # "could not run".  The mutation drops every comment and quote from
        # the fixture, BuiltinFunctions.cpp's literals included.
        kept = {}
        for path in sources + [os.path.join(fx, BUILTINS_FILE)]:
            with open(path, encoding="utf-8") as fh:
                kept[path] = fh.read()
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(kept[path].split("//")[0].replace('"', "") + "\n")
        if all(open(p, encoding="utf-8").read() == kept[p] for p in kept):
            failures.append("SELFTEST: gate/stripper-blanks-nothing: mutation "
                            "did not apply")
        gate("stripper-blanks-nothing", base_graph, 2, "stripper")
        for path, text in kept.items():
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
        gate("legal-after-comment-restore", base_graph, 0)

        # ENC-1579, check E: the acceptance's three cases on an engine file,
        # then the fixture control.  Each asserts its edit applied.
        target = sources[0]
        for label, line, want in (
                ("market-type-identifier", "MarketTickComputer* e_x;\n", 1),
                ("market-type-in-comment", "// MarketTickComputer\n", 0),
                ("market-type-in-string",
                 'const char* e_s = "MarketTickComputer";\n', 0)):
            before = append_line(f"E-{label}", target, line)
            gate(f"E-{label}", base_graph, want)
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(before)
        fixture = os.path.join(fx, VOCAB_FIXTURE)
        with open(fixture, encoding="utf-8") as fh:
            fixture_text = fh.read()
        os.remove(fixture)
        if os.path.exists(fixture):
            failures.append("SELFTEST: gate/E-fixture-deleted: mutation did "
                            "not apply")
        gate("E-fixture-deleted", base_graph, 2, "positive control")
        # A fixture whose code lines are all commented out hits nothing.
        with open(fixture, "w", encoding="utf-8") as fh:
            fh.write("".join("// " + ln for ln in
                             fixture_text.splitlines(keepends=True)))
        gate("E-fixture-hits-nothing", base_graph, 2, "positive control")
        # A word moved into code on a DECOY line is a leak, not a hit.
        with open(fixture, "w", encoding="utf-8") as fh:
            fh.write(fixture_text.replace("int d2;", "int vwap;"))
        gate("E-fixture-decoy-leak", base_graph, 2, "positive control")
        vocab_path = os.path.join(fx, VOCAB_FILE)
        with open(fixture, "w", encoding="utf-8") as fh:
            fh.write(fixture_text)
        with open(vocab_path, encoding="utf-8") as fh:
            vocab_text = fh.read()
        os.remove(vocab_path)
        gate("E-vocabulary-deleted", base_graph, 2, "vocabulary")
        with open(vocab_path, "w", encoding="utf-8") as fh:
            fh.write(vocab_text)
        gate("legal-after-E-restore", base_graph, 0)

        # Scanned, never linked: the stub or the control target in a real one.
        gate("stub-in-krn_engine",
             mutant("stub-in-krn_engine",
                    lambda g: g["krn_engine"].append(CONTROL_PROBE)), 1)
        gate("control-in-krn_engine",
             mutant("control-in-krn_engine",
                    lambda g: g["krn_engine"].append(CONTROL_TARGET)), 1)
        # ...and in a target none of the checked roots reach, which only the
        # control_reachers rule can see.
        gate("control-in-unreached-target",
             mutant("control-in-unreached-target",
                    lambda g: g.update({"krn_other": [CONTROL_TARGET]})),
             1)
        # Check F's control (ENC-1491) is scanned, never linked, the same way.
        with_nm_ctl = dict(base_graph, **{NM_CONTROL_TARGET: []})
        gate("nm-control-row-is-legal", with_nm_ctl, 0)
        gate("nm-control-in-krn_host",
             mutant("nm-control-in-krn_host", lambda g: g.update(
                 {NM_CONTROL_TARGET: [],
                  "krn_host": ["krn_engine", NM_CONTROL_TARGET]})), 1)
    return len(ran)


# --- check F: the built artifact, read with nm (ENC-1491) -----------------
# Checks A-E read source text and the CMake link graph.  A graph walk is only
# as complete as its registry, and ENC-1481's history is two registries with a
# hole one level below where it was looked for.  Check F asks the linker
# instead: it runs `nm -C` over the archives and the executable this build
# produced and fails on a forbidden symbol, defined or undefined (an undefined
# arrow:: reference is the dependency just as surely as a defined one).  It
# corroborates checks C and D from the other direction; it does not replace
# them, because they name the offending CHAIN and this names only a symbol.
#
# WHAT IT FORBIDS, re-derived for the repo after the extraction (no data/, no
# Arrow).  Each rule comes from vocabulary this file already owns, so there
# is no second list to drift:
#
#   third-party  one rule per FORBIDDEN_INCLUDE_PREFIXES entry: `arrow/` is
#                the namespace `arrow::`, `arrow_` a C-API symbol prefix,
#                `duckdb` both `duckdb::` and `duckdb_`.  The namespace must be
#                at the top level, so `krn::arrowlike::` and `myarrow::` pass.
#   krn_data     a `krn::data::X` whose X is not declared inside a
#                `namespace krn::data` / `namespace data` block of the engine's
#                own include/ tree.  Not the bare namespace: SymbolTable is
#                engine code in krn::data on purpose (ENC-1486), so the
#                allowlist is read off include/ at run time, never typed in.
#   market       the connector namespace `krn::market::`, and any component of
#                a `krn::`-anchored qualified name that is check E's derived
#                vocabulary (ENC-1579), less its exemptions.  Anchored on
#                `krn::` because third-party code says `Level` too:
#                rapidjson's Writer::Level is in libkrn_engine.a.
#
# ARTIFACTS are krn_engine, krn_host and krn_engine_server, read from the
# manifest's `artifact.<target>` rows (`$<TARGET_FILE:...>`).
#
# POSITIVE CONTROL: krn_isolation_nm_control, built from
# tools/fixtures/nm_control_probe.cpp with krn_engine's IPO setting, so it is
# the same object format.  Its probes must trip EVERY rule and its decoys
# none.  Scanned, never linked, like krn_isolation_control.
#
# COULD NOT RUN (exit 2, never a pass): no nm, nm exits non-zero or says it
# needs a plugin, an artifact is missing, an artifact shows fewer than
# MIN_ARTIFACT_KRN_SYMBOLS defined krn:: symbols (a slim-LTO archive read by a
# nm without the plugin lists none, and would otherwise read as clean), or the
# control is silent on any rule or trips on a decoy.
ARTIFACT_ROOTS = ("krn_engine", "krn_host", "krn_engine_server")
NM_CONTROL_TARGET = "krn_isolation_nm_control"
NM_CONTROL_SOURCE = os.path.join("tools", "fixtures", "nm_control_probe.cpp")
NM_PROBE = "krn_isolation_nm_probe"
NM_DECOY = "krn_isolation_nm_probe_decoy"
# Measured 2026-11-15, Release + LTO, gcc-nm: 727 defined krn:: symbols in
# libkrn_engine.a, 58 in libkrn_host.a, 400 in krn_engine_server.  20 is well
# under the smallest and well over the zero of an archive nm cannot read
# (`nm --plugin /nonexistent` on a slim-LTO object lists nothing).  A plain
# nm reads these archives here only because binutils autoloads the plugin
# from bfd-plugins/; that is why CMake prefers gcc-nm / llvm-nm.
MIN_ARTIFACT_KRN_SYMBOLS = 20
NM_LINE_RE = re.compile(r"^\s*([0-9a-fA-F]*)\s+([A-Za-z?-])\s(.+)$")
KRN_QUALIFIED_RE = re.compile(r"(?<![\w:])krn::((?:\w+::)*)(\w+)")
KRN_DATA_RE = re.compile(r"(?<![\w:])krn::data::(\w+)")
MARKET_NS_RE = re.compile(r"(?<![\w:])krn::market::")
_LEAD_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def third_party_symbol_rules(prefixes=FORBIDDEN_INCLUDE_PREFIXES
                             ) -> list[tuple[str, re.Pattern]]:
    """(label, pattern) derived from the forbidden include prefixes."""
    rules: list[tuple[str, re.Pattern]] = []
    seen: set[str] = set()
    for p in prefixes:
        m = _LEAD_IDENT_RE.match(p)
        if not m:
            raise ValueError(f"include prefix {p!r} has no leading identifier")
        ident, rest = m.group(0), p[m.end():]
        if rest == "" and ident.endswith("_"):
            forms = [("c", ident)]                     # arrow_
        elif rest == "":
            forms = [("ns", ident), ("c", ident + "_")]  # duckdb
        else:
            forms = [("ns", ident)]                    # arrow/, clickhouse-cpp/
        for kind, word in forms:
            label = f"{word}::" if kind == "ns" else f"{word}*"
            if label in seen:
                continue
            seen.add(label)
            pat = (re.compile(rf"(?<![\w:]){re.escape(word)}::") if kind == "ns"
                   else re.compile(rf"^{re.escape(word)}"))
            rules.append((label, pat))
    return rules


def engine_data_names(root: str) -> set[str]:
    """Names the engine's include/ declares inside krn::data.  The allowlist."""
    names: set[str] = set()
    for path in walk_tree(root, "include"):
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                text = strip_cpp(fh.read())
        except OSError:
            continue
        for m in re.finditer(r"\bnamespace\s+(?:krn::data|data)\s*\{", text):
            depth, i = 1, m.end()
            while i < len(text) and depth:
                depth += {"{": 1, "}": -1}.get(text[i], 0)
                i += 1
            body = text[m.end():i]
            names |= set(re.findall(
                r"\b(?:class|struct|union|enum(?:\s+class|\s+struct)?)\s+(\w+)",
                body))
            names |= set(re.findall(r"\busing\s+(\w+)\s*=", body))
            names |= set(re.findall(r"\bnamespace\s+(\w+)", body))
    return names


def symbol_rules(root: str) -> list[tuple[str, object]]:
    """Every check F rule as (label, fn symbol -> list of offending strings)."""
    rules: list[tuple[str, object]] = []
    for label, pat in third_party_symbol_rules():
        rules.append((label, lambda s, pat=pat:
                      [m.group(0) for m in pat.finditer(s)]))
    allowed = engine_data_names(root)
    rules.append(("krn_data", lambda s: [
        m.group(0) for m in KRN_DATA_RE.finditer(s)
        if m.group(1) not in allowed]))
    rules.append(("krn::market::", lambda s:
                  [m.group(0) for m in MARKET_NS_RE.finditer(s)]))
    try:
        vocab = read_vocabulary(os.path.join(root, VOCAB_FILE))
    except OSError:
        vocab = {}
    match = vocab_matcher({w: t for w, t in vocab.items()
                           if w not in VOCAB_EXEMPT})

    def market_words(s: str) -> list[str]:
        out = []
        for m in KRN_QUALIFIED_RE.finditer(s):
            for comp in m.group(1).split("::")[:-1] + [m.group(2)]:
                word = match(comp)
                if word is not None:
                    out.append(f"{m.group(0)} ({word})")
        return out
    rules.append(("market vocabulary", market_words))
    return rules


def parse_nm(text: str) -> list[tuple[str, str]]:
    """(type letter, demangled name) per symbol line of `nm -C` output."""
    out = []
    for line in text.splitlines():
        m = NM_LINE_RE.match(line)
        if m:
            out.append((m.group(2), m.group(3).strip()))
    return out


def run_nm(nm: str, path: str) -> tuple[list[tuple[str, str]] | None, str]:
    """(symbols, '') or (None, why it could not be read)."""
    import subprocess
    if not path or not os.path.isfile(path):
        return None, f"artifact {path!r} does not exist -- build it first"
    try:
        res = subprocess.run([nm, "-C", path], capture_output=True, text=True,
                             check=False)
    except OSError as exc:
        return None, f"cannot run nm {nm!r}: {exc}"
    if res.returncode != 0:
        return None, (f"nm {nm!r} exited {res.returncode} on {path}: "
                      f"{res.stderr.strip()[:300]}")
    if "plugin needed" in res.stderr:
        return None, (f"nm {nm!r} cannot read the LTO objects in {path} "
                      f"(\"plugin needed\") -- use gcc-nm or llvm-nm")
    return parse_nm(res.stdout), ""


def scan_symbols(symbols: list[tuple[str, str]], rules
                 ) -> dict[str, list[str]]:
    """rule label -> offending symbol lines."""
    hits: dict[str, list[str]] = {label: [] for label, _ in rules}
    for kind, name in symbols:
        for label, fn in rules:
            found = fn(name)
            if found:
                hits[label].append(f"{kind} {name}  [{', '.join(found)}]")
    return hits


def defined_krn_symbols(symbols: list[tuple[str, str]]) -> int:
    return sum(1 for kind, name in symbols
               if kind not in ("U", "w", "v") and name.startswith("krn::"))


def evaluate_artifacts(manifest: dict[str, str], nm: str | None = None
                       ) -> tuple[list[str], list[str], list[str]]:
    """(vacuous, problems, summary) for check F.  Pure apart from running nm."""
    import shutil
    summary = ["krn_engine artifact isolation (ENC-1491, check F, SPEC D14)"]
    vacuous: list[str] = []
    problems: list[str] = []
    root = manifest.get("root", "")
    if not root or not os.path.isdir(root):
        return ([f"manifest 'root' is missing or not a directory: {root!r}"],
                [], summary)
    nm = nm or manifest.get("nm", "")
    resolved = (shutil.which(nm) if nm and os.sep not in nm
                else (nm if nm and os.access(nm, os.X_OK) else None))
    summary.append(f"  nm              : {nm or '(none)'} -> "
                   f"{resolved or 'NOT FOUND'}")
    if not resolved:
        return ([f"no usable nm ({nm or 'the manifest has no nm= row'}) -- "
                 f"THE SPEC D14 INVARIANT WAS NOT CHECKED AGAINST THE BUILT "
                 f"ARTIFACTS: nothing has read krn_engine, krn_host or "
                 f"krn_engine_server for Arrow, ClickHouse-client, krn_data or "
                 f"market symbols"], [], summary)
    rules = symbol_rules(root)
    labels = [label for label, _ in rules]
    summary.append(f"  rules           : {labels}")
    summary.append(f"  krn::data allow : {sorted(engine_data_names(root))} "
                   f"(declared in include/)")

    # The control first: a rule it cannot trip is a rule nobody has seen work.
    ctl_path = manifest.get("nm_control", "")
    ctl, why = run_nm(resolved, ctl_path)
    if ctl is None:
        vacuous.append(f"check F's positive control {NM_CONTROL_TARGET} "
                       f"could not be read: {why}")
        summary.append(f"  positive control: MISSING ({why})")
    else:
        probes = [s for s in ctl if NM_PROBE in s[1] and NM_DECOY not in s[1]]
        decoys = [s for s in ctl if NM_DECOY in s[1]]
        p_hits = scan_symbols(probes, rules)
        d_hits = scan_symbols(decoys, rules)
        silent = [label for label in labels if not p_hits[label]]
        dead = [n for k, n in probes if not any(fn(n) for _, fn in rules)]
        leaks = [h for label in labels for h in d_hits[label]]
        bad = []
        if not probes or not decoys:
            bad.append(f"{len(probes)} probe(s), {len(decoys)} decoy(s) -- "
                       f"the control was not built from {NM_CONTROL_SOURCE}")
        if silent:
            bad.append(f"no probe tripped rule(s) {silent}")
        if dead:
            bad.append(f"probe(s) tripping no rule: {dead}")
        if leaks:
            bad.append(f"decoy(s) tripped a rule: {leaks}")
        if bad:
            vacuous.append(f"check F's positive control {NM_CONTROL_TARGET} "
                           f"failed: {'; '.join(bad)}")
            summary.append(f"  positive control: FAILED ({'; '.join(bad)})")
        else:
            summary.append(
                f"  positive control: ok ({NM_CONTROL_TARGET}: {len(probes)} "
                f"probes trip all {len(labels)} rules "
                f"{ {label: len(p_hits[label]) for label in labels} }, "
                f"{len(decoys)} decoys trip none)")

    for target in ARTIFACT_ROOTS:
        path = manifest.get(f"artifact.{target}", "")
        syms, why = run_nm(resolved, path)
        if syms is None:
            vacuous.append(f"{target}: {why}")
            summary.append(f"  {target:<16}: COULD NOT READ ({why})")
            continue
        defined = defined_krn_symbols(syms)
        hits = scan_symbols(syms, rules)
        summary.append(
            f"  {target:<16}: {path} -- {len(syms)} symbols, {defined} "
            f"defined krn::, forbidden "
            f"{ {label: len(hits[label]) for label in labels} }")
        if defined < MIN_ARTIFACT_KRN_SYMBOLS:
            vacuous.append(
                f"{target}: nm shows {defined} defined krn:: symbols in {path} "
                f"(floor {MIN_ARTIFACT_KRN_SYMBOLS}) -- nm is not reading this "
                f"artifact's objects, so a clean result proves nothing")
        for label in labels:
            for h in hits[label][:5]:
                problems.append(
                    f"{target} ({os.path.basename(path)}) has a forbidden "
                    f"symbol, rule {label!r}: {h} -- the built artifact carries "
                    f"what SPEC D14's live-only variant must not")
            if len(hits[label]) > 5:
                problems.append(f"{target}: ... and {len(hits[label]) - 5} more "
                                f"{label!r} symbol(s)")
    return vacuous, problems, summary


def run_artifact_gate(manifest_path: str, nm: str | None) -> int:
    try:
        manifest = read_manifest(manifest_path)
    except OSError as exc:
        print(f"error: cannot read manifest {manifest_path!r}: {exc}",
              file=sys.stderr)
        return 2
    vacuous, problems, summary = evaluate_artifacts(manifest, nm)
    for line in summary:
        print(line)
    if vacuous:
        print("\nCOULD NOT RUN -- check F refuses to report a pass "
              "(exit 2 unless a violation is also found):", file=sys.stderr)
        for v in vacuous:
            print(f"  ! {v}", file=sys.stderr)
    if problems:
        print(f"\nFAILED: {len(problems)} forbidden symbol(s) in the built "
              f"artifacts:", file=sys.stderr)
        for p in problems:
            print(f"  ! {p}", file=sys.stderr)
    code = exit_code(vacuous, problems)
    if code == 0:
        print("\nOK: nm finds no Arrow/Parquet/Gandiva/ClickHouse/DuckDB, "
              "krn_data or market symbol in krn_engine, krn_host or "
              "krn_engine_server, and the control trips every rule.")
    return code


def selftest_artifacts(failures: list[str], log: list[str]) -> int:
    """Check F's rules on synthetic nm lines, then the whole check F gate,
    exit code included, against fake nm executables."""
    import stat
    import tempfile

    ran = 0
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)

    # Every forbidden include prefix yields a symbol rule.
    tp = [label for label, _ in third_party_symbol_rules()]
    for p in FORBIDDEN_INCLUDE_PREFIXES:
        ident = _LEAD_IDENT_RE.match(p).group(0).rstrip("_")
        if not any(label.startswith(ident) for label in tp):
            failures.append(f"SELFTEST: F/prefix-{p}: no symbol rule derived")
    if set(tp) != {"arrow::", "arrow_*", "parquet::", "gandiva::",
                   "clickhouse::", "duckdb::", "duckdb_*"}:
        failures.append(f"SELFTEST: F/derived-rules: {tp}")
    log.append(f"  F/derived-rules: {tp}")
    ran += 1
    if "SymbolTable" not in engine_data_names(root):
        failures.append("SELFTEST: F/krn-data-allowlist: include/ declares "
                        "krn::data::SymbolTable but the allowlist misses it")
    ran += 1

    rules = symbol_rules(root)
    # (name, demangled symbol, rule label it must trip or None)
    cases = (
        ("arrow-ns", "arrow::Status::OK()", "arrow::"),
        ("arrow-in-template-arg", "std::shared_ptr<arrow::Table>::get() const",
         "arrow::"),
        ("arrow-c", "arrow_array_release", "arrow_*"),
        ("parquet", "parquet::ParquetFileReader::Open()", "parquet::"),
        ("gandiva", "gandiva::Projector::Make()", "gandiva::"),
        ("clickhouse", "clickhouse::Client::Execute(clickhouse::Query const&)",
         "clickhouse::"),
        ("duckdb-ns", "duckdb::Connection::Query()", "duckdb::"),
        ("duckdb-c", "duckdb_open", "duckdb_*"),
        ("krn-data-record", "krn::data::TickRecord::size() const", "krn_data"),
        ("krn-data-as-arg",
         "krn::Dispatcher::onRecord(krn::data::RecordFieldSource const&)",
         "krn_data"),
        ("market-ns", "krn::market::registerWith()", "krn::market::"),
        ("market-type", "krn::ob::OrderBookManager::apply()",
         "market vocabulary"),
        ("market-prefix", "krn::ta::sma_20()", "market vocabulary"),
        ("clean-symboltable", "krn::data::SymbolTable::intern(std::"
         "basic_string_view<char, std::char_traits<char> >)", None),
        ("clean-not-top-level", "krn::arrowlike::f()", None),
        ("clean-other-ns", "myarrow::f()", None),
        ("clean-c-infix", "garrow_array_new", None),
        ("clean-third-party-word", "rapidjson::Writer<rapidjson::"
         "StringBuffer>::Level::Level()", None),
        ("clean-exempt-word", "krn::stats::mean(std::vector<double> const&)",
         None),
        ("clean-engine", "krn::Dispatcher::onTick(krn::Event const&)", None),
    )
    for name, sym, want in cases:
        hits = scan_symbols([("T", sym)], rules)
        got = [label for label, v in hits.items() if v]
        ok = got == ([want] if want else [])
        log.append(f"  F/{name}: {got}")
        if not ok:
            failures.append(f"SELFTEST: F/{name}: {sym!r} tripped {got}, "
                            f"expected {[want] if want else []}")
        ran += 1
    if parse_nm("0000 T a::b()\n         U c::d(int)\nx.o:\n\n") != [
            ("T", "a::b()"), ("U", "c::d(int)")]:
        failures.append("SELFTEST: F/parse-nm: defined and undefined lines")
    ran += 1

    # The whole gate.  An "artifact" is a text file of nm output, and the fake
    # nm cats it, so every refusal below is the gate's and not the fixture's.
    with tempfile.TemporaryDirectory(prefix="enc1491-nm-") as tmp:
        fake = os.path.join(tmp, "fake-nm")
        with open(fake, "w", encoding="utf-8") as fh:
            fh.write('#!/bin/sh\n[ -f "$2" ] || exit 1\ncat "$2"\n')
        os.chmod(fake, os.stat(fake).st_mode | stat.S_IEXEC)
        failing = os.path.join(tmp, "failing-nm")
        with open(failing, "w", encoding="utf-8") as fh:
            fh.write("#!/bin/sh\necho 'nm: boom' >&2\nexit 3\n")
        os.chmod(failing, os.stat(failing).st_mode | stat.S_IEXEC)
        pluginless = os.path.join(tmp, "pluginless-nm")
        with open(pluginless, "w", encoding="utf-8") as fh:
            fh.write("#!/bin/sh\necho \"nm: x.o: plugin needed to handle lto "
                     "object\" >&2\necho '00000000 T __gnu_lto_slim'\n")
        os.chmod(pluginless, os.stat(pluginless).st_mode | stat.S_IEXEC)

        legal = "".join(f"00000000 T krn::Dispatcher::f{i}()\n"
                        for i in range(MIN_ARTIFACT_KRN_SYMBOLS + 5))
        legal += ("         U krn::data::SymbolTable::name(unsigned int) "
                  "const\n00000000 W rapidjson::Writer<int>::Level::Level()\n")
        # The control's real symbols, from the committed fixture.
        with open(os.path.join(root, NM_CONTROL_SOURCE), encoding="utf-8") as fh:
            src = fh.read()
        control = "".join([
            "00000000 T arrow::krn_isolation_nm_probe()\n",
            "00000000 T arrow_krn_isolation_nm_probe\n",
            "00000000 T parquet::krn_isolation_nm_probe()\n",
            "00000000 T gandiva::krn_isolation_nm_probe()\n",
            "00000000 T clickhouse::krn_isolation_nm_probe()\n",
            "00000000 T duckdb::krn_isolation_nm_probe()\n",
            "00000000 T duckdb_krn_isolation_nm_probe\n",
            "00000000 T krn::data::krn_isolation_nm_probe("
            "krn::data::TickRecord const*)\n",
            "00000000 T krn::market::krn_isolation_nm_probe()\n",
            "00000000 T krn::nm_probe::krn_isolation_nm_probe("
            "krn::nm_probe::OrderBookManager const*)\n",
            "00000000 T krn_isolation_nm_probe_decoy("
            "krn::data::SymbolTable const*)\n",
            "00000000 T myarrow::krn_isolation_nm_probe_decoy()\n",
        ])
        if NM_PROBE not in src or NM_DECOY not in src:
            failures.append("SELFTEST: F/control-source: the committed fixture "
                            "names no probe or no decoy")

        def write(name, text):
            p = os.path.join(tmp, name)
            with open(p, "w", encoding="utf-8") as fh:
                fh.write(text)
            return p

        def manifest(arts, ctl, nm_path=fake):
            m = {"root": root, "nm": nm_path, "nm_control": write("ctl", ctl)}
            for t in ARTIFACT_ROOTS:
                m[f"artifact.{t}"] = write(t, arts.get(t, legal))
            return m

        def gate(name, m, want, mention=None, nm=None):
            vac, probs, _summary = evaluate_artifacts(m, nm)
            got = exit_code(vac, probs)
            log.append(f"  F/gate-{name}: exit {got}, want {want}")
            if got != want:
                failures.append(f"SELFTEST: F/gate-{name} exited {got}, "
                                f"expected {want} (vacuous={vac}, "
                                f"problems={probs})")
            if mention and not any(mention in x for x in vac + probs):
                failures.append(f"SELFTEST: F/gate-{name} did not say "
                                f"{mention!r}: {vac + probs}")

        def applied(name, before, after):
            if before == after:
                failures.append(f"SELFTEST: F/gate-{name}: mutation did not "
                                f"apply")
            return after

        gate("legal", manifest({}, control), 0)
        for t, line in (("krn_engine", "         U arrow::Status::OK()\n"),
                        ("krn_host", "00000000 T krn::data::TickRecord::x()\n"),
                        ("krn_engine_server",
                         "00000000 T krn::market::MarketConnector::start()\n")):
            art = applied(f"{t}-forbidden", legal, legal + line)
            gate(f"{t}-forbidden", manifest({t: art}, control), 1, t)
        gate("nm-missing", manifest({}, control,
                                    os.path.join(tmp, "no-such-nm")),
             2, "NOT CHECKED")
        gate("nm-row-absent", {k: v for k, v in manifest({}, control).items()
                               if k != "nm"}, 2, "NOT CHECKED")
        gate("nm-fails", manifest({}, control, failing), 2, "exited 3")
        gate("nm-needs-plugin", manifest({}, control, pluginless), 2,
             "plugin needed")
        m = manifest({}, control)
        m["artifact.krn_host"] = os.path.join(tmp, "not-built.a")
        gate("artifact-missing", m, 2, "does not exist")
        slim = applied("slim-lto-archive", legal,
                       "00000000 T __gnu_lto_slim\n")
        gate("slim-lto-archive", manifest({"krn_engine": slim}, control), 2,
             "floor")
        gate("violation-outranks-vacuity",
             manifest({"krn_engine": slim + "         U duckdb_open\n"},
                      control), 1, "duckdb_*")
        # The control: missing, silent on one rule, dead probe, leaking decoy.
        m = manifest({}, control)
        m.pop("nm_control")
        gate("control-missing", m, 2, NM_CONTROL_TARGET)
        no_gandiva = applied("control-silent-rule", control, "".join(
            ln + "\n" for ln in control.splitlines() if "gandiva" not in ln))
        gate("control-silent-rule", manifest({}, no_gandiva), 2, "gandiva::")
        dead = applied("control-dead-probe", control,
                       control + "00000000 T krn::reference::"
                       "krn_isolation_nm_probe()\n")
        gate("control-dead-probe", manifest({}, dead), 2, "tripping no rule")
        leak = applied("control-decoy-leak", control,
                       control + "00000000 T arrow::"
                       "krn_isolation_nm_probe_decoy()\n")
        gate("control-decoy-leak", manifest({}, leak), 2, "decoy")
        gate("legal-after", manifest({}, control), 0)
        ran += 16
    return ran


def selftest() -> int:
    """Each predicate, shown failing on an input that violates it.

    Without this, a refactor that broke a predicate would turn the whole check
    into a green no-op and nothing would say so.
    """
    import tempfile

    failures: list[str] = []

    def expect_detected(name: str, problems: list[str]) -> None:
        if not problems:
            failures.append(
                f"SELFTEST: {name} did NOT flag a violating input -- the check "
                f"is a no-op and would pass a real violation")

    def expect_clean(name: str, problems: list[str]) -> None:
        if problems:
            failures.append(
                f"SELFTEST: {name} flagged a LEGAL input: {problems}")

    with tempfile.TemporaryDirectory(prefix="enc1481-isolation-") as tmp:
        # A miniature repo with the real shape: an engine include tree that
        # legitimately holds krn/engine/SymbolTable.hpp, and a data/include tree that
        # holds krn/data/Schema.hpp.
        os.makedirs(os.path.join(tmp, "include", "krn", "engine"))
        os.makedirs(os.path.join(tmp, "data", "include", "krn", "data"))
        os.makedirs(os.path.join(tmp, "src", "core"))
        open(os.path.join(tmp, "include", "krn", "engine", "SymbolTable.hpp"), "w").close()
        open(os.path.join(tmp, "data", "include", "krn", "data", "Schema.hpp"), "w").close()

        os.makedirs(os.path.join(tmp, "connectors", "market", "include", "krn", "market"))
        open(os.path.join(tmp, "connectors", "market", "include", "krn",
                          "market", "MarketConnector.hpp"), "w").close()

        good = os.path.join(tmp, "src", "core", "Ok.cpp")
        with open(good, "w", encoding="utf-8") as fh:
            fh.write('#include "krn/engine/SymbolTable.hpp"\n#include <vector>\n')

        os.makedirs(os.path.join(tmp, "plugins", "reference", "include", "krn", "reference"))
        open(os.path.join(tmp, "plugins", "reference", "include", "krn",
                          "reference", "SyntheticConnector.hpp"), "w").close()

        foreign = non_engine_header_roots(tmp)
        expect_clean("A/legal-SymbolTable.hpp", check_a_includes(tmp, [good], foreign))

        # apps/ (krn_engine_server, ENC-1561): a plugin header is legal, a
        # market or Arrow header is not.
        app_roots = app_foreign_roots(tmp, foreign)
        if len(app_roots) != len(foreign) - 1:
            failures.append(f"SELFTEST: app_foreign_roots did not drop exactly "
                            f"plugins/reference/include: {app_roots}")
        os.makedirs(os.path.join(tmp, APPS_DIR, "engine_server"))
        for label, inc, bad in (
                ("plugin", "krn/reference/SyntheticConnector.hpp", False),
                ("market", "krn/market/MarketConnector.hpp", True),
                ("arrow", "arrow/api.h", True)):
            app = os.path.join(tmp, APPS_DIR, "engine_server", f"{label}.cpp")
            with open(app, "w", encoding="utf-8") as fh:
                fh.write(f'#include "{inc}"\n')
            got = check_a_includes(tmp, [app], app_roots, "engine server")
            (expect_detected if bad else expect_clean)(
                f"A/engine-server-includes-{label}", got)
        plugin_user = os.path.join(tmp, "UsesPlugin.cpp")
        with open(plugin_user, "w", encoding="utf-8") as fh:
            fh.write('#include "krn/reference/SyntheticConnector.hpp"\n')
        expect_detected("A/engine-includes-a-plugin-header",
                        check_a_includes(tmp, [plugin_user], foreign))

        # ENC-1570: the apps/ list with NO connector root is empty, and that
        # is intended -- the prefix rule still bites on it.
        no_conn = app_foreign_roots(tmp, [r for r in foreign if not _under(
            os.path.realpath(r), os.path.realpath(os.path.join(tmp,
                                                               "connectors")))])
        if no_conn != []:
            failures.append(f"SELFTEST: A/apps-roots-empty-without-connectors: "
                            f"{no_conn}")
        arrow_app = os.path.join(tmp, APPS_DIR, "engine_server", "arrow.cpp")
        expect_detected("A/engine-server-includes-arrow-with-empty-roots",
                        check_a_includes(tmp, [arrow_app], no_conn,
                                         "engine server"))

        # src/main.cpp left with krn_server (ENC-1570): one re-added is engine
        # code -- walked, and held to the engine's rule, not a weaker one.
        main_cpp = os.path.join(tmp, "src", "main.cpp")
        with open(main_cpp, "w", encoding="utf-8") as fh:
            fh.write('#include "krn/market/MarketConnector.hpp"\n')
        walked = set(walk_engine_trees(tmp))
        if os.path.realpath(main_cpp) not in walked:
            failures.append("SELFTEST: walk_engine_trees skipped src/main.cpp")
        if os.path.realpath(good) not in walked:
            failures.append("SELFTEST: walk_engine_trees missed a real engine source")
        expect_detected("A/src-main.cpp-is-engine-code",
                        check_a_includes(tmp, sorted(walked), foreign))
        expect_detected("cross/data-source-in-engine-target",
                        check_cmake_sources_stay_in_the_engine_trees(
                            tmp, [os.path.join(tmp, "data", "src", "Schema.cpp")], walked))
        expect_clean("cross/legal",
                     check_cmake_sources_stay_in_the_engine_trees(tmp, [good], walked))

        bad_arrow = os.path.join(tmp, "src", "core", "BadArrow.cpp")
        with open(bad_arrow, "w", encoding="utf-8") as fh:
            fh.write("#include <arrow/api.h>\n")
        expect_detected("A/arrow-include",
                        check_a_includes(tmp, [bad_arrow], foreign))

        # ENC-1578: the dead data/include root is DROPPED, not filtered.  The
        # fixture has a data/include tree; it must not be a root, and every
        # root returned must exist, so no named root can vanish in silence.
        if not os.path.isdir(os.path.join(tmp, "data", "include")):
            failures.append("SELFTEST: A/dead-data-root-dropped: the fixture "
                            "has no data/include, so this proves nothing")
        if any(_under(os.path.realpath(r), os.path.realpath(
                os.path.join(tmp, "data"))) for r in foreign):
            failures.append(f"SELFTEST: A/dead-data-root-dropped: data/include "
                            f"is still a root: {foreign}")
        if not foreign or not all(os.path.isdir(r) for r in foreign):
            failures.append(f"SELFTEST: A/dead-data-root-dropped: a root is "
                            f"missing or empty: {foreign}")

        # --- B: the include path, its ONE exemption, and that exemption's
        # --- bound.  Both directions, because B's defect was a FALSE
        # --- POSITIVE: the same commit failed from the shared checkout and
        # --- passed from a worktree (ENC-1497).
        engine_inc = os.path.join(tmp, "include")
        third = os.path.join(tmp, VENDOR_SUBDIR)
        expect_clean("B/legal", check_b_include_dirs(tmp, [engine_inc]))

        # LEGAL, direction 1 -- the resolution a fresh `git worktree` gets.
        # third_party/ is gitignored, so it is absent and CMake's find_path
        # falls through to the system rapidjson. Out-of-repo dirs are not B's
        # business; this was already green and must stay green.
        expect_clean("B/external-rapidjson-is-legal",
                     check_b_include_dirs(tmp, [engine_inc, "/usr/include"]))

        # LEGAL, direction 2 -- the resolution the SHARED CHECKOUT gets for
        # that same commit, where third_party/ is populated. THIS IS THE CASE
        # THAT WAS RED. Both spellings find_path accepts are pinned, because
        # CMakeLists.txt hints at both `third_party/rapidjson` and
        # `third_party/rapidjson/include`.
        os.makedirs(os.path.join(third, "rapidjson", "include"))
        expect_clean("B/vendored-rapidjson-is-legal",
                     check_b_include_dirs(
                         tmp, [engine_inc, os.path.join(third, "rapidjson")]))
        expect_clean("B/vendored-rapidjson-include-subdir-is-legal",
                     check_b_include_dirs(
                         tmp, [engine_inc,
                               os.path.join(third, "rapidjson", "include")]))

        # THE BOUND ON THE EXEMPTION. A vendored tree that names a forbidden
        # dependency is still refused, so the hole cannot carry Arrow onto the
        # engine's include path. An exemption with no stated bound is how this
        # comes back.
        for vendored in ("arrow", "clickhouse-cpp", "parquet", "duckdb",
                         os.path.join("apache-arrow-18.0.0", "cpp", "src")):
            os.makedirs(os.path.join(third, vendored), exist_ok=True)
            expect_detected(
                f"B/vendored-{vendored.replace(os.sep, '-')}-is-not-exempt",
                check_b_include_dirs(
                    tmp, [engine_inc, os.path.join(third, vendored)]))

        # The exemption is decided on os.path.realpath, so a foreign include
        # root symlinked under third_party/ resolves back out of it and fails
        # on the general rule rather than slipping through.
        sneaky = os.path.join(third, "sneaky")
        os.symlink(os.path.join(tmp, "data", "include"), sneaky)
        expect_detected("B/symlink-out-of-third_party-is-not-exempt",
                        check_b_include_dirs(tmp, [engine_inc, sneaky]))

        # And it is a PATH prefix, not a string prefix: a sibling directory
        # whose name merely starts with "third_party" is not vendored.
        expect_detected("B/third_party-sibling-dir-is-not-exempt",
                        check_b_include_dirs(
                            tmp, [engine_inc,
                                  os.path.join(tmp, VENDOR_SUBDIR + "_evil",
                                               "include")]))

        # WHAT CHECK B IS ACTUALLY FOR, and what must keep failing: the data
        # layer's include root and every connector's.
        expect_detected("B/data-include-on-engine-path",
                        check_b_include_dirs(
                            tmp, [engine_inc,
                                  os.path.join(tmp, "data", "include")]))
        os.makedirs(os.path.join(tmp, "plugins", "reference", "include"),
                    exist_ok=True)
        for parent, conn in (("connectors", "market"), ("plugins", "reference")):
            expect_detected(
                f"B/{parent}-{conn}-include-on-engine-path",
                check_b_include_dirs(
                    tmp, [engine_inc,
                          os.path.join(tmp, parent, conn, "include")]))
        # A bare src/ on the engine's include path is not vendored either --
        # the general rule still covers everything outside the two cases above.
        expect_detected("B/src-on-engine-path",
                        check_b_include_dirs(
                            tmp, [engine_inc, os.path.join(tmp, "src")]))
        expect_detected("B/include-missing", check_b_include_dirs(tmp, ["/usr/include"]))
        # ...including when the only in-repo dir present is a legal vendored
        # one: the exemption must not stand in for include/ itself.
        expect_detected("B/include-missing-but-vendored-present",
                        check_b_include_dirs(
                            tmp, [os.path.join(third, "rapidjson")]))

        # classify_include_dir feeds the summary line that makes a differing
        # verdict between two checkouts explicable at all, so it is pinned too.
        for path, want in ((os.path.join(third, "rapidjson"), "vendored"),
                           (engine_inc, "in-repo"),
                           ("/usr/include", "external"),
                           ("", "not reported")):
            got = classify_include_dir(tmp, path)
            if want not in got:
                failures.append(
                    f"SELFTEST: classify_include_dir({path!r}) = {got!r}, "
                    f"expected it to say {want!r}")

        # --- C and D: the link closure ----------------------------------
        # The real shape of this repo's link graph after ENC-1570, from the
        # `link.*` rows of build/krn_engine_isolation.manifest -- OpenSSL
        # included, and with no decoration, because the real rows carry none.
        legal = {
            "krn": ["krn_engine"],
            "krn_engine": ["Boost::system", "Threads::Threads",
                           "RapidJSON::rapidjson", "OpenSSL::SSL",
                           "OpenSSL::Crypto"],
            "krn_plugin_reference": ["krn_engine"],
            "krn_host": ["krn_engine"],
            "krn_engine_server": ["krn_host", "krn_plugin_reference"],
            "krn_tests": ["krn_engine", "Boost::system", "Threads::Threads",
                          "RapidJSON::rapidjson", "GTest::gtest_main",
                          "krn_plugin_reference"],
        }
        for r in CHECKED_ROOTS:
            expect_clean(f"D/closure-legal-{r}", forbidden_links(legal, r))
        expect_clean("D/market-legal", market_reachers(legal))

        # D11.1 after ENC-1570: no graphed target reaches the market
        # connector -- each root directly and through the umbrella, and a
        # target no root reaches, which only market_reachers sees.
        for r, label, extra in (
                ("krn_engine", "direct", {}),
                ("krn_host", "direct", {}),
                ("krn_host", "via-umbrella", {"krn": ["krn_engine",
                                                      "krn_connector_market"]}),
                ("krn_engine_server", "direct", {}),
                ("krn_engine_server", "via-umbrella",
                 {"krn": ["krn_engine", "krn_connector_market"]}),
                ("krn_tests", "direct", {}),
                ("krn_tests", "via-umbrella",
                 {"krn": ["krn_engine", "krn_connector_market"]}),
                ("bench_x", "unreached", {})):
            lib = "krn" if label == "via-umbrella" else "krn_connector_market"
            mut = dict(legal, **extra)
            mut[r] = legal.get(r, []) + [lib]
            if mut == legal:
                failures.append(f"SELFTEST: D/{r}-market-{label}: "
                                f"mutation did not apply")
            got = market_reachers(mut)
            expect_detected(f"D/{r}-links-market-{label}",
                            [p for p in got if p.startswith(f"{r} links")])
            expect_clean(f"D/{r}-links-market-{label}-is-reported-once",
                         [p for p in forbidden_links(mut, r)
                          if "connector_market" in p])

        # THE CASE THAT MOTIVATED THE CLOSURE. One line on krn_host makes the
        # live-only variant unlinkable; krn_engine_server's own property does
        # not change at all.
        intermediate = dict(legal, krn_host=["krn_engine", "krn_data"])
        expect_detected("D/intermediate-links-krn_data",
                        forbidden_links(intermediate, "krn_engine_server"))
        expect_clean("C/intermediate-does-not-touch-the-engine",
                     forbidden_links(intermediate, "krn_engine"))

        # CONTRAST, not a gate: the review finding in one assertion. A check
        # that reads only krn_engine_server's OWN link libraries sees
        # ["krn_host", "krn_plugin_reference"] and passes the graph above. If
        # this ever stops being true the contrast is stale and should go.
        direct_only = [link_item_name(x)
                       for x in intermediate["krn_engine_server"]]
        if any(p.search(n) for n in direct_only for p in FORBIDDEN_LINK_PATTERNS):
            failures.append(
                "SELFTEST: the one-level contrast case is no longer one-level "
                "-- D/intermediate-links-krn_data no longer demonstrates why "
                "the closure walk exists")

        # The second route in: Arrow on the plugin. It does NOT make the
        # engine depend on Arrow (C stays clean, correctly) but it does make
        # the live-only server unlinkable, so D must redden.
        plugin_arrow = dict(legal, krn_plugin_reference=["krn_engine",
                                                         "krn::arrow"])
        expect_detected("D/plugin-links-arrow",
                        forbidden_links(plugin_arrow, "krn_engine_server"))
        expect_clean("C/plugin-arrow-is-not-an-engine-dependency",
                     forbidden_links(plugin_arrow, "krn_engine"))

        # The third route, and the one the FIRST VERSION OF THIS CHECK MISSED:
        # a first-party library with no row is a leaf the walk never expands.
        new_lib = dict(legal, krn_plugin_reference=["krn_engine", "krn_store"])
        expect_detected("D/unregistered-first-party-library",
                        forbidden_links(new_lib, "krn_engine_server"))
        aliased = dict(legal, krn_plugin_reference=["krn_engine", "krn::data"])
        expect_detected("D/alias-for-krn_data",
                        forbidden_links(aliased, "krn_engine_server"))
        # ... and once the row exists, the walk reaches through it.
        registered = dict(new_lib, krn_store=["krn_engine", "krn_data"])
        expect_detected("D/through-a-registered-new-library",
                        forbidden_links(registered, "krn_engine_server"))
        clean_new_lib = dict(new_lib, krn_store=["krn_engine"])
        expect_clean("D/a-registered-new-library-that-is-clean",
                     forbidden_links(clean_new_lib, "krn_engine_server"))
        # A genuinely third-party leaf must NOT trip the first-party rule.
        expect_clean("D/third-party-leaves-are-leaves",
                     forbidden_links({"krn_engine_server": [
                         "Boost::system", "m", "GTest::gtest_main",
                         "stdc++fs", "libm.so"]}, "krn_engine_server"))

        # Direct links still caught, one per forbidden pattern.
        for name in ("krn_data", "krn::arrow", "arrow_static", "parquet",
                     "clickhouse-cpp", "duckdb"):
            expect_detected(f"C/direct-{name}",
                            forbidden_links({"krn_engine": ["Threads::Threads",
                                                            name]},
                                            "krn_engine"))

        # DECORATED ENTRIES.  These forms are not observed in the manifest
        # yet (the observed one, BUILD_INTERFACE, is pinned below; see the
        # note above link_item_name).  They are here because every one of
        # them was a working bypass of the first version of this check.
        for label, entry in (
                ("genex/LINK_ONLY", "$<LINK_ONLY:krn_data>"),
                ("genex/IF", "$<IF:$<BOOL:ON>,krn_data,>"),
                ("genex/LINK_LIBRARY-feature",
                 "$<LINK_LIBRARY:WHOLE_ARCHIVE,krn_data>"),
                ("genex/nested-head", "$<$<CONFIG:Debug>:krn_data>"),
                ("genex/arrow-wrapped", "$<LINK_ONLY:krn::arrow>")):
            expect_detected(f"C/{label} (defensive: not emitted today)",
                            forbidden_links({"krn_engine": [entry]},
                                            "krn_engine"))
        # OBSERVED, not defensive (ENC-1585): the real manifest row from
        # ENC-1561's experiment (c).  It must be caught on krn_host and,
        # through it, on krn_engine_server.
        observed = dict(legal, krn_host=split_list(
            "krn_engine|$<BUILD_INTERFACE:krn_connector_market>"))
        if observed == legal:
            failures.append("SELFTEST: D/build-interface-wrapped-market-link: "
                            "mutation did not apply")
        got = market_reachers(observed)
        for r in ("krn_host", "krn_engine_server"):
            expect_detected(f"D/build-interface-wrapped-market-link-{r}",
                            [p for p in got if p.startswith(f"{r} links")])
        # A wrapped edge must still be TRAVERSED, not merely matched.
        expect_detected("D/wrapped-edge-is-still-walked (defensive)",
                        forbidden_links(
                            {"krn_engine_server":
                             ["$<LINK_LIBRARY:WHOLE_ARCHIVE,krn>"],
                             "krn": ["krn_data"]},
                            "krn_engine_server"))
        expect_detected("C/directory-scoped-interface-entry (defensive)",
                        forbidden_links(
                            {"krn_engine_server": ["krn::@(0x55f0)"],
                             "krn": ["krn_data"]},
                            "krn_engine_server"))
        # And a decorated LEGAL entry must not be mistaken for a violation.
        expect_clean("C/decorated-legal-entry (defensive)",
                     forbidden_links(
                         {"krn_engine": ["$<LINK_ONLY:Boost::headers>",
                                         "$<LINK_LIBRARY:WHOLE_ARCHIVE,m>"]},
                         "krn_engine"))

        # A cycle must terminate rather than hang, and must still report.
        expect_detected("closure/cycle-safe",
                        forbidden_links({"a": ["b"], "b": ["a", "krn_data"]}, "a"))

        # krn_data's closure must trip a pattern: it is forbidden by name and
        # by what it links, wherever it is built from.
        expect_detected("control/krn_data-reaches-arrow",
                        forbidden_links(
                            {"krn_data": ["krn_engine", "krn::arrow"],
                             "krn_engine": ["Threads::Threads"]},
                            "krn_data"))

    gate_cases = selftest_gate(failures)
    nm_log: list[str] = []
    nm_cases = selftest_artifacts(failures, nm_log)
    print(f"check F nm rules and gate ({nm_cases} cases):")
    for line in nm_log:
        print(line)
    strip_log: list[str] = []
    strip_cases = selftest_stripper(failures, strip_log)
    strip_cases += selftest_vocabulary(failures, strip_log)
    print(f"check E stripper and vocabulary ({strip_cases} cases):")
    for line in strip_log:
        print(line)

    if failures:
        for f in failures:
            print(f, file=sys.stderr)
        print(f"\nSELFTEST FAILED: {len(failures)} predicate(s) are not gating.",
              file=sys.stderr)
        return 1
    print("selftest: all predicates detect their violation and pass legal input "
          f"({gate_cases} whole-gate exit-code cases, {strip_cases} check E "
          f"stripper and vocabulary cases, {nm_cases} check F nm cases)")
    return 0


def evaluate(manifest: dict[str, str]
             ) -> tuple[list[str], list[str], list[str]]:
    """(vacuous, problems, summary) for one manifest.  Pure: prints nothing.

    Split out of main() so the selftest can drive the whole gate, verdict and
    exit code included, against a fixture manifest (ENC-1507).
    """
    summary: list[str] = []
    root = manifest.get("root", "")
    if not root or not os.path.isdir(root):
        return ([f"manifest 'root' is missing or not a directory: {root!r}"],
                [], summary)

    include_dirs = dedupe(
        split_list(manifest.get("engine_include_dirs", "")) +
        split_list(manifest.get("engine_interface_include_dirs", "")))
    graph = parse_link_graph(manifest)
    link_libs = graph.get("krn_engine", [])
    engine_closure = link_closure(graph, "krn_engine")
    host_closure = link_closure(graph, "krn_host")
    app_closure = link_closure(graph, "krn_engine_server")
    tests_closure = (link_closure(graph, "krn_tests")
                     if "krn_tests" in graph else [])
    cmake_sources = dedupe(split_list(manifest.get("engine_cmake_sources", "")))
    host_sources = dedupe(split_list(manifest.get("host_cmake_sources", "")))
    app_cmake_sources = dedupe(
        split_list(manifest.get("engine_server_cmake_sources", "")))
    rapidjson_dir = manifest.get("rapidjson_include_dir", "")

    walked = walk_engine_trees(root)
    walked_set = set(walked)
    # Scan the union: CMake's list is authoritative for what krn_engine
    # compiles, and the walk catches a header CMake's glob pattern happens not
    # to match.
    sources = sorted(walked_set | {os.path.realpath(s)
                                   for s in cmake_sources + host_sources
                                   if s.endswith(SOURCE_SUFFIXES)})
    foreign_roots = non_engine_header_roots(root)
    # krn_engine_server: the apps/ walk plus CMake's own list for the target.
    app_foreign = app_foreign_roots(root, foreign_roots)
    app_walked = walk_tree(root, APPS_DIR)
    app_sources = sorted(set(app_walked) | {
        os.path.realpath(s) for s in app_cmake_sources
        if s.endswith(SOURCE_SUFFIXES)})

    # --- anti-vacuity: an empty input is a failure, not a pass --------------
    vacuous: list[str] = []
    # Both counts, separately, and that is the point. `sources` is the UNION
    # of the walk and CMake's own source list, so CMake alone supplies ~99 and a
    # floor on the union cannot detect a walk that returned nothing -- which is
    # the failure the floor is named for. The walk gets its own floor.
    if len(walked) < MIN_ENGINE_FILES:
        vacuous.append(
            f"the filesystem walk of src/ and include/ found {len(walked)} "
            f"source/header files (floor {MIN_ENGINE_FILES}) -- the traversal "
            f"is blind, and because `sources` is the union of the walk and "
            f"CMake's list, a clean check A result would otherwise look "
            f"perfectly normal")
    if len(sources) < MIN_ENGINE_FILES:
        vacuous.append(
            f"scanned only {len(sources)} engine source/header files (floor "
            f"{MIN_ENGINE_FILES}) -- the traversal found nothing, so a clean "
            f"result proves nothing")
    if len(include_dirs) < MIN_ENGINE_INCLUDE_DIRS:
        vacuous.append(
            f"krn_engine reported {len(include_dirs)} include directories -- "
            f"the CMake manifest did not resolve")
    if not link_libs:
        vacuous.append(
            "krn_engine reported no link libraries -- the CMake manifest did "
            "not resolve (it links at least Threads::Threads)")
    if not foreign_roots:
        vacuous.append(
            "found no non-engine include roots (connectors/*/include, "
            "plugins/*/include) -- check A cannot "
            "detect a layering "
            "violation it has no tree to resolve against")
    if not cmake_sources:
        vacuous.append(
            "the manifest lists no krn_engine sources -- the generator "
            "expression did not resolve, so the cross-check is inert")
    # The gate's roots (ENC-1561, D11.2).  CMake refuses to configure without
    # them; a manifest that lacks one is stale or hand-made.
    for r in REQUIRED_ROOTS:
        if r not in graph:
            vacuous.append(
                f"the manifest has no link.{r} row -- {r} is a root of this "
                f"gate, so checks C/D were not run on it")
    names = {name for name, _, _ in app_closure}
    if not {"krn_host", "krn_engine"} <= names:
        vacuous.append(
            "krn_engine_server's link closure does not reach krn_host and "
            "krn_engine -- it goes krn_engine_server -> krn_host -> "
            "krn_engine, so check D on it never traversed an edge")
    if not any(name == "krn_engine" for name, _, _ in host_closure):
        vacuous.append(
            "krn_host's link closure does not reach krn_engine -- check D on "
            "krn_host never traversed an edge")
    if not app_sources:
        vacuous.append(
            f"found no krn_engine_server source under {APPS_DIR}/ or in the "
            f"manifest -- its include scan has nothing to check")
    if not app_cmake_sources:
        vacuous.append(
            "the manifest lists no krn_engine_server sources -- the generator "
            "expression did not resolve, so files outside apps/ are unscanned")
    # POSITIVE CONTROL (ENC-1507): the patterns must be shown to match
    # something in THIS build before a clean engine verdict means anything.
    # The primary control is krn_isolation_control, which exists in every
    # configuration; its absence is "could not run", never a pass.
    if CONTROL_TARGET not in graph:
        vacuous.append(
            f"the manifest has no link.{CONTROL_TARGET} row -- the positive "
            f"control is missing, so nothing shows the forbidden patterns can "
            f"match anything in this build and a clean verdict proves nothing "
            f"(CMakeLists.txt must declare it in every configuration)")
        control = f"MISSING ({CONTROL_TARGET} is not in the manifest)"
    else:
        ctl_hits = forbidden_links(graph, CONTROL_TARGET)
        ctl_probe = any(CONTROL_PROBE in cands for _, cands, _ in
                        link_closure(graph, CONTROL_TARGET))
        if not ctl_hits or not ctl_probe:
            vacuous.append(
                f"{CONTROL_TARGET}'s closure "
                f"{'trips no forbidden pattern' if not ctl_hits else 'does not reach ' + CONTROL_PROBE} "
                f"-- the control is silent or mis-declared, so checks C and D "
                f"are not shown to detect anything")
            control = f"FAILED ({CONTROL_TARGET} does not fire)"
        else:
            control = (f"ok ({CONTROL_TARGET} -> {CONTROL_PROBE}: "
                       f"{len(ctl_hits)} forbidden link(s) seen)")
    # Check E, part 1 (ENC-1578): strip check A's own list.  No vocabulary
    # yet; a stripper that blanks nothing, or breaks a file's shape or an
    # #include path, cannot be trusted with one.
    stripped, strip_stats, strip_bad = strip_sources(sources)
    if strip_bad:
        vacuous.append(f"the check E stripper damaged {len(strip_bad)} "
                       f"file(s): {strip_bad[:5]}")
    if sources and strip_stats["blanked"] == 0:
        vacuous.append(f"the check E stripper blanked nothing in "
                       f"{len(sources)} files -- it is not running")
    # Check E, part 2 (ENC-1579): the derived vocabulary, and its fixture
    # control, which runs whatever the build options.
    e_vacuous, e_problems, e_stats = check_e(root, stripped)
    vacuous += e_vacuous

    problems = (check_a_includes(root, sources, foreign_roots) +
                check_a_includes(root, app_sources, app_foreign,
                                 "engine server (krn_engine_server)") +
                check_cmake_sources_stay_in_the_engine_trees(
                    root, cmake_sources, walked_set) +
                check_cmake_sources_stay_in_the_engine_trees(
                    root, host_sources, walked_set, "krn_host") +
                check_b_include_dirs(root, include_dirs) +
                [p for r in CHECKED_ROOTS if r in graph
                 for p in forbidden_links(graph, r)] +
                market_reachers(graph) +
                control_reachers(graph) +
                e_problems)

    summary.append("krn_engine isolation (ENC-1481, SPEC D14)")
    summary.append(f"  root            : {root}")
    summary.append(f"  engine files    : {len(sources)} "
                   f"({len(cmake_sources)} from CMake, {len(walked)} walked, "
                   f"{len(host_sources)} krn_host)")
    summary.append(f"  engine server   : {len(app_sources)} file(s) "
                   f"({len(app_cmake_sources)} from CMake, "
                   f"{len(app_walked)} under {APPS_DIR}/)")
    summary.append(f"  include dirs    : {len(include_dirs)}")
    # Printed unconditionally and on both verdicts: it is the field that
    # explains why two checkouts of one commit can disagree (ENC-1497).
    summary.append(f"  rapidjson       : {classify_include_dir(root, rapidjson_dir)}")
    summary.append(f"  link libraries  : {len(link_libs)} engine (direct)")
    summary.append(f"  link closure    : {len(engine_closure)} from krn_engine, "
                   f"{len(host_closure)} from krn_host, "
                   f"{len(app_closure)} from krn_engine_server, "
                   f"{len(tests_closure)} from krn_tests, over "
                   f"{len(graph)} graphed target(s): {sorted(graph)}")
    summary.append(f"  market-free     : every graphed target "
                   f"({len(graph) - (CONTROL_TARGET in graph)}; "
                   f"{MARKET_CONNECTOR_LINK.pattern})")
    summary.append(f"  positive control: {control}")
    reachers = control_reachers(graph)
    summary.append(f"  control linked  : "
                   f"{'by ' + str(len(reachers)) + ' target(s)' if reachers else 'by nothing'}"
                   f" (closure of all {len(graph) - (CONTROL_TARGET in graph)} "
                   f"other graphed target(s) read)")
    summary.append(f"  foreign roots   : {[os.path.relpath(r, root) for r in foreign_roots]}")
    summary.append(
        f"  apps/ rule      : {len(app_sources)} file(s) against include "
        f"prefixes {list(FORBIDDEN_INCLUDE_PREFIXES)} and connector roots "
        f"{[os.path.relpath(r, root) for r in app_foreign]}"
        + ("" if app_foreign else
           " (empty is intended: no connectors/ in this repo, ENC-1570)"))
    summary.append(
        f"  E stripper      : {strip_stats['files']} files (check A's list), "
        f"{strip_stats['blanked']} chars blanked, {strip_stats['includes']} "
        f"#include paths kept verbatim, {strip_stats['includes_in_comments']} "
        f"in comments; D11 sentinel {D11_SENTINEL_RE.pattern}: "
        f"{strip_stats['sentinel_before']} before, "
        f"{strip_stats['sentinel_after']} after (reported, not gated)")
    summary.append(
        f"  E vocabulary    : {e_stats['words']} derived words, "
        f"{len(VOCAB_EXEMPT)} exempt, {len(e_problems)} hit(s) in engine "
        f"code; exempt hits {dict(sorted(e_stats['exempt_hits'].items()))}")
    summary.append(f"  E control       : {e_stats['fixture']}")

    return vacuous, problems, summary


def exit_code(vacuous: list[str], problems: list[str]) -> int:
    """1 on a violation, else 2 if the gate could not run, else 0."""
    if problems:
        return 1
    if vacuous:
        return 2
    return 0


def run_gate(manifest_path: str) -> int:
    try:
        manifest = read_manifest(manifest_path)
    except OSError as exc:
        print(f"error: cannot read manifest {manifest_path!r}: {exc}",
              file=sys.stderr)
        return 2
    vacuous, problems, summary = evaluate(manifest)
    for line in summary:
        print(line)

    if vacuous:
        print("\nCOULD NOT RUN -- vacuous input, refusing to report a pass "
              "(exit 2 unless a violation is also found):", file=sys.stderr)
        for v in vacuous:
            print(f"  ! {v}", file=sys.stderr)

    if problems:
        print(f"\nFAILED: {len(problems)} isolation violation(s):", file=sys.stderr)
        for p in problems:
            print(f"  ! {p}", file=sys.stderr)
        print("\nArrow and the ClickHouse client must never become krn_engine "
              "dependencies (SPEC D14, krn-data's docs/data-layer.md §4 rule 6): the "
              "live-only variant of D14's client profile omits krn_data "
              "entirely and must still link.", file=sys.stderr)

    code = exit_code(vacuous, problems)
    if code == 0:
        print("\nOK: no Arrow/ClickHouse include in engine code or apps/, no "
              "connector or plugin header reached from the engine, engine "
              "include path is include/ or a non-forbidden vendored tree, no "
              "data-layer library in the TRANSITIVE link closure of any checked "
              "root, and no market connector in any graphed target's.")
    return code


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest",
                    help="key=value file generated by CMake with krn_engine's "
                         "resolved include dirs and sources, plus one "
                         "link.<target> / interface_link.<target> row per "
                         "first-party target (the link graph checks C and D "
                         "walk)")
    ap.add_argument("--selftest", action="store_true",
                    help="verify each predicate flags a violating input")
    ap.add_argument("--artifacts", action="store_true",
                    help="check F: read the built artifacts named by the "
                         "manifest's artifact.<target> rows with nm -C")
    ap.add_argument("--nm", help="nm to use for --artifacts (default: the "
                                 "manifest's nm= row)")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if not args.manifest:
        print("error: --manifest is required (or pass --selftest)", file=sys.stderr)
        return 2
    if args.artifacts:
        return run_artifact_gate(args.manifest, args.nm)
    return run_gate(args.manifest)


if __name__ == "__main__":
    sys.exit(main())
