cmake_minimum_required(VERSION 3.20)
project(KRN LANGUAGES CXX)

# ---- Consumed mode (ENC-1553, purity SPEC D5) ----
# Under FetchContent/add_subdirectory this file defines ONLY the engine library
# targets: no tests, benchmarks, server, install rules or isolation gate. 3.20 predates PROJECT_IS_TOP_LEVEL.
#
# The two build switches are shadowed by NORMAL variables in this directory
# scope, never written with CACHE FORCE: option() is a no-op over a normal
# variable (CMP0077 NEW under 3.20), and a normal variable set here is invisible
# to the consumer's scope. So a consumer's own -DKRN_BUILD_TESTS=ON, or its own
# option of that name, keeps its value in its cache and in its scope.
if (CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
  set(_krn_top_level ON)
else()
  set(_krn_top_level OFF)
  foreach (_krn_opt IN ITEMS KRN_BUILD_TESTS KRN_BUILD_BENCHMARKS)
    set(${_krn_opt} OFF)
  endforeach()
  message(STATUS
    "krn: consumed by ${CMAKE_PROJECT_NAME} — engine library targets only")
endif()

# ---- Default build type (ENC-1548) ----
# An empty CMAKE_BUILD_TYPE on a single-config generator compiles with no
# optimisation flags at all, and the Release block below (-O3, -march baseline,
# LTO) never runs. Default it to Release, the configuration the quickstart and
# stack.sh pass, matching krn-data (ENC-1586). Top-level and single-config only.
# A non-empty -DCMAKE_BUILD_TYPE always wins, FORCE only replaces the empty entry
# project() creates, and a consumer's build type is never touched.
get_property(_krn_multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if (_krn_top_level AND NOT _krn_multi_config AND "${CMAKE_BUILD_TYPE}" STREQUAL "")
  set(CMAKE_BUILD_TYPE Release CACHE STRING
    "Build type (single-config generators). Defaulted to Release when empty." FORCE)
  message(STATUS "krn: CMAKE_BUILD_TYPE was empty, defaulted to Release")
endif()

# ---- Options ----
option(KRN_BUILD_TESTS "Build unit tests in tests/" OFF)
option(KRN_BUILD_BENCHMARKS "Build Google Benchmark suite in benchmarks/" OFF)
option(KRN_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)

# Microarchitecture (ENC-790 / H5): -march=native pins the binary to the exact
# build CPU and SIGILLs on older hosts. Release/RelWithDebInfo therefore default
# to a portable baseline (KRN_ARCH_BASELINE, x86-64-v2 ≈ any 2009+ x86_64 CPU);
# -march=native is opt-in via KRN_NATIVE_ARCH. -O3 and LTO are kept regardless.
option(KRN_NATIVE_ARCH
  "Build Release with -march=native (non-portable: pins the binary to the build CPU; SIGILL on other hosts)"
  OFF)
set(KRN_ARCH_BASELINE "x86-64-v2" CACHE STRING
  "Portable -march baseline used for Release on x86_64 when KRN_NATIVE_ARCH is OFF (empty string disables -march entirely)")

# Sanitizers (ENC-801 / M15): opt-in, default off so a plain `cmake .. && make`
# is byte-for-byte unaffected. When set, the flags are injected into compile AND
# link for every target in this build (engine, connectors, server, tests, gtest).
set(KRN_SANITIZE "off" CACHE STRING
  "Enable a sanitizer for all targets: off|address|thread|undefined|address+undefined")
set_property(CACHE KRN_SANITIZE PROPERTY STRINGS off address thread undefined address+undefined)

# ---- C++ standard ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---- Platform defs (Beast/Asio on Windows) ----
if (WIN32)
  add_definitions(-D_WIN32_WINNT=0x0A00) # Windows 10
endif()

# ---- Third-party root
# Override with: -DTHIRD_PARTY_ROOT=C:/wherever
if (NOT DEFINED THIRD_PARTY_ROOT)
  if (WIN32)
    set(THIRD_PARTY_ROOT "C:/thirdparty")
  else()
    set(THIRD_PARTY_ROOT "${PROJECT_SOURCE_DIR}/third_party")
  endif()
endif()

# ---- Threads
find_package(Threads REQUIRED)

# ---- OpenSSL (optional — enables wss:// in WsFeedClient)
find_package(OpenSSL QUIET)
if (OpenSSL_FOUND)
  message(STATUS "OpenSSL found: ${OPENSSL_VERSION} — wss:// feed support enabled")
else()
  message(STATUS "OpenSSL not found — wss:// feed support disabled (ws:// still works)")
endif()

# =======================
# Sanitizers (opt-in; ENC-801 / M15)
# =======================
# Injected at directory scope BEFORE any target is defined, so every target
# created below (including FetchContent's googletest/benchmark) is instrumented
# in both compile and link. Default "off" => no flags => default build unchanged.
if (NOT KRN_SANITIZE STREQUAL "off")
  if (MSVC)
    message(WARNING "KRN_SANITIZE=${KRN_SANITIZE} is not supported with this MSVC toolchain — ignoring")
  else()
    if (KRN_SANITIZE STREQUAL "address")
      set(_KRN_SAN_FLAGS -fsanitize=address -fno-omit-frame-pointer)
    elseif (KRN_SANITIZE STREQUAL "thread")
      set(_KRN_SAN_FLAGS -fsanitize=thread -fno-omit-frame-pointer)
    elseif (KRN_SANITIZE STREQUAL "undefined")
      set(_KRN_SAN_FLAGS -fsanitize=undefined -fno-omit-frame-pointer)
    elseif (KRN_SANITIZE STREQUAL "address+undefined")
      set(_KRN_SAN_FLAGS -fsanitize=address,undefined -fno-omit-frame-pointer)
    else()
      message(FATAL_ERROR
        "KRN_SANITIZE='${KRN_SANITIZE}' is invalid — use off|address|thread|undefined|address+undefined")
    endif()
    add_compile_options(${_KRN_SAN_FLAGS})
    add_link_options(${_KRN_SAN_FLAGS})
    message(STATUS "Sanitizer enabled for all targets: ${KRN_SANITIZE}")
  endif()
endif()

# =======================
# Warning flags helper (ENC-806 / L12)
# =======================
# Applies the standard warning set to a target. -Werror stays OFF unless the
# opt-in KRN_WARNINGS_AS_ERRORS is set, so concurrent source edits never break
# the central build just by emitting a warning.
function(krn_apply_warnings tgt)
  if (MSVC)
    target_compile_options(${tgt} PRIVATE /W4)
    if (KRN_WARNINGS_AS_ERRORS)
      target_compile_options(${tgt} PRIVATE /WX)
    endif()
  else()
    target_compile_options(${tgt} PRIVATE -Wall -Wextra -Wpedantic)
    if (KRN_WARNINGS_AS_ERRORS)
      target_compile_options(${tgt} PRIVATE -Werror)
    endif()
  endif()
endfunction()

# =======================
# Boost (prefer normal install)
# =======================
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.75 QUIET COMPONENTS system)

set(USE_HEADER_ONLY_BOOST OFF)
if (NOT Boost_FOUND)
  # Fallback to header-only using your local unpacked folder
  set(BOOST_FALLBACK_INCLUDE "${THIRD_PARTY_ROOT}/boost_1_88_0")
  if (EXISTS "${BOOST_FALLBACK_INCLUDE}/boost/beast/core.hpp")
    message(STATUS "Using header-only Boost from: ${BOOST_FALLBACK_INCLUDE}")
    set(USE_HEADER_ONLY_BOOST ON)

    add_library(Boost::headers INTERFACE IMPORTED)
    target_include_directories(Boost::headers INTERFACE "${BOOST_FALLBACK_INCLUDE}")

    # Provide a stub 'Boost::system' since <boost/system/error_code.hpp> is header-only
    add_library(Boost::system INTERFACE IMPORTED)
    target_link_libraries(Boost::system INTERFACE Boost::headers)
  else()
    message(FATAL_ERROR
      "Boost not found and no fallback at ${BOOST_FALLBACK_INCLUDE}. "
      "Install Boost or set -DTHIRD_PARTY_ROOT=C:/thirdparty")
  endif()
else()
  message(STATUS "Found Boost: ${Boost_INCLUDE_DIRS}")
endif()

# =======================
# RapidJSON (header-only)
# =======================
# We want #include <rapidjson/document.h>
# Accept both:
#   C:/thirdparty/rapidjson            (contains folder 'rapidjson/')
#   C:/thirdparty/rapidjson/include    (contains folder 'rapidjson/')
set(RAPIDJSON_HINTS
  "${RAPIDJSON_ROOT}"
  "$ENV{RAPIDJSON_ROOT}"
  "${THIRD_PARTY_ROOT}/rapidjson"
  "${THIRD_PARTY_ROOT}/rapidjson/include"
)

find_path(RAPIDJSON_INCLUDE_DIR
  NAMES rapidjson/document.h
  HINTS ${RAPIDJSON_HINTS}
)

if (NOT RAPIDJSON_INCLUDE_DIR)
  message(FATAL_ERROR
    "RapidJSON headers not found. Set RAPIDJSON_ROOT or place them under "
    "${THIRD_PARTY_ROOT}/rapidjson so that ${RAPIDJSON_INCLUDE_DIR}/rapidjson/document.h exists.")
endif()

if (NOT TARGET RapidJSON::rapidjson)  # a consumer may have defined it already
  add_library(RapidJSON::rapidjson INTERFACE IMPORTED)
  target_include_directories(RapidJSON::rapidjson INTERFACE "${RAPIDJSON_INCLUDE_DIR}")
endif()
message(STATUS "RapidJSON include dir: ${RAPIDJSON_INCLUDE_DIR}")

# =======================
# Sources / targets
# =======================
#   krn_engine             — domain-agnostic core (src/**, include/krn/**)
#   krn                    — INTERFACE alias for krn_engine (legacy consumer target)
# The market connector and krn_server are krn-common's (purity SPEC D6, D8;
# ENC-1570). krn_engine's PUBLIC include paths contain *only* the engine
# headers, so engine code that references a plugin header fails to compile.

# Explicit lists, not globs (ENC-1551): a missing file is a configure error,
# and paths are rooted at this project, not at whoever add_subdirectory()s it.
set(KRN_ENGINE_SOURCES
  src/core/AtomicStore.cpp
  src/core/BuiltinFunctions.cpp
  src/core/Dispatcher.cpp
  src/core/DispatcherRecord.cpp
  src/core/Expr.cpp
  src/core/FunctionMap.cpp
  src/core/JsonValidator.cpp
  src/core/RequestRegistry.cpp
  src/core/TreeBuilder.cpp
  src/data/SymbolTable.cpp
  src/engine/IEventComputer.cpp
  src/forum/ConnectorsClient.cpp
  src/nodes/Aggregate.cpp
  src/nodes/AtomicAccessor.cpp
  src/nodes/BucketTime.cpp
  src/nodes/Expr.cpp
  src/nodes/Field.cpp
  src/nodes/Filter.cpp
  src/nodes/GroupSplit.cpp
  src/nodes/Interval.cpp
  src/nodes/Listener.cpp
  src/nodes/Pack.cpp
  src/nodes/Responder.cpp
  src/nodes/Switch.cpp
  src/nodes/Tee.cpp
  src/nodes/TumblingWindow.cpp
  src/nodes/VectorReducer.cpp
  src/nodes/Worker.cpp
  src/rt/Strand.cpp
  src/rt/ThreadPool.cpp
  src/server/ClientSession.cpp
  src/server/WebSocketServer.cpp
  src/util/Config.cpp
  src/util/Logger.cpp
  src/util/Metrics.cpp
  src/ws/ClientConnection.cpp
  src/ws/WSResponder.cpp
  src/ws/WsBridge.cpp
)
list(TRANSFORM KRN_ENGINE_SOURCES PREPEND "${PROJECT_SOURCE_DIR}/")

set(KRN_ENGINE_HEADERS
  include/krn/AtomicStore.hpp
  include/krn/Dispatcher.hpp
  include/krn/Event.hpp
  include/krn/ExecutionContext.hpp
  include/krn/Expr.hpp
  include/krn/FunctionMap.hpp
  include/krn/FunctionRegistry.hpp
  include/krn/JsonValidator.hpp
  include/krn/Metrics.hpp
  include/krn/NodeRegistry.hpp
  include/krn/RequestRegistry.hpp
  include/krn/Result.hpp
  include/krn/Span.hpp
  include/krn/StreamValue.hpp
  include/krn/TreeBuilder.hpp
  include/krn/atomic/AtomicProviderRegistry.hpp
  include/krn/engine/ConfigNamespaceRegistry.hpp
  include/krn/engine/EngineRegistries.hpp
  include/krn/engine/EventComputerRegistry.hpp
  include/krn/engine/EventTypeRegistry.hpp
  include/krn/engine/FieldSource.hpp
  include/krn/engine/IConnector.hpp
  include/krn/engine/IEventComputer.hpp
  include/krn/engine/IngressRegistry.hpp
  include/krn/engine/JsonFieldSource.hpp
  include/krn/engine/NodeTypeRegistry.hpp
  include/krn/engine/ProtocolIngressRegistry.hpp
  include/krn/engine/Registries.hpp
  include/krn/engine/SymbolTable.hpp
  include/krn/forum/ConnectorsClient.hpp
  include/krn/nodes/Aggregate.hpp
  include/krn/nodes/AtomicAccessor.hpp
  include/krn/nodes/BucketTime.hpp
  include/krn/nodes/Expr.hpp
  include/krn/nodes/Field.hpp
  include/krn/nodes/Filter.hpp
  include/krn/nodes/GroupSplit.hpp
  include/krn/nodes/INode.hpp
  include/krn/nodes/InputPort.hpp
  include/krn/nodes/Interval.hpp
  include/krn/nodes/JoinBy.hpp
  include/krn/nodes/Listener.hpp
  include/krn/nodes/NodeEnv.hpp
  include/krn/nodes/Pack.hpp
  include/krn/nodes/Responder.hpp
  include/krn/nodes/Switch.hpp
  include/krn/nodes/Tee.hpp
  include/krn/nodes/TumblingWindow.hpp
  include/krn/nodes/VectorReducer.hpp
  include/krn/nodes/Worker.hpp
  include/krn/rt/SPSCQueue.hpp
  include/krn/rt/Strand.hpp
  include/krn/rt/ThreadPool.hpp
  include/krn/runtime/IStoppable.hpp
  include/krn/runtime/ShutdownCoordinator.hpp
  include/krn/server/ClientSession.hpp
  include/krn/server/RequestKey.hpp
  include/krn/server/WebSocketServer.hpp
  include/krn/util/Config.hpp
  include/krn/util/JsonUtil.hpp
  include/krn/util/Logger.hpp
  include/krn/util/Metrics.hpp
  include/krn/ws/ClientConnection.hpp
  include/krn/ws/WSResponder.hpp
  include/krn/ws/WsBridge.hpp
)
list(TRANSFORM KRN_ENGINE_HEADERS PREPEND "${PROJECT_SOURCE_DIR}/")


# ---- Engine library ----
add_library(krn_engine STATIC ${KRN_ENGINE_SOURCES} ${KRN_ENGINE_HEADERS})
add_library(krn::krn_engine ALIAS krn_engine)  # the name find_package(krn) gives
target_include_directories(krn_engine PUBLIC
  "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>")
target_compile_features(krn_engine PUBLIC cxx_std_20)

if (Boost_FOUND)
  target_include_directories(krn_engine PUBLIC ${Boost_INCLUDE_DIRS})
  target_link_libraries(krn_engine PUBLIC Boost::system)
else()
  target_link_libraries(krn_engine PUBLIC Boost::system)
  target_compile_definitions(krn_engine PUBLIC
    BOOST_ALL_NO_LIB
    BOOST_ERROR_CODE_HEADER_ONLY
    BOOST_SYSTEM_NO_DEPRECATED
    BOOST_ASIO_NO_DEPRECATED
    BOOST_BEAST_USE_STD_STRING_VIEW
  )
endif()

target_link_libraries(krn_engine PUBLIC Threads::Threads RapidJSON::rapidjson)

if (OpenSSL_FOUND)
  target_link_libraries(krn_engine PUBLIC OpenSSL::SSL OpenSSL::Crypto)
  target_compile_definitions(krn_engine PUBLIC KRN_HAS_SSL=1)
endif()

# ---- Legacy umbrella (top-level only); the benchmarks link it ----
if (_krn_top_level)
  add_library(krn INTERFACE)
  target_link_libraries(krn INTERFACE krn_engine)
endif()

# ---- Synthetic connector (demo): proves the zero-core-changes invariant ----
# Not linked into `krn` — kept as an opt-in target that test code and demos
# pull in explicitly. Adding a connector requires (a) this library target and
# (b) one `registerWith` call at the composition root; no engine edits.
set(KRN_SYNTHETIC_SOURCES
  plugins/reference/src/ReferenceComputer.cpp
  plugins/reference/src/ReferenceConnector.cpp
  plugins/reference/src/SyntheticConnector.cpp
)
list(TRANSFORM KRN_SYNTHETIC_SOURCES PREPEND "${PROJECT_SOURCE_DIR}/")
set(KRN_SYNTHETIC_HEADERS
  plugins/reference/include/krn/reference/ReferenceComputer.hpp
  plugins/reference/include/krn/reference/ReferenceConnector.hpp
  plugins/reference/include/krn/reference/SyntheticConnector.hpp
)
list(TRANSFORM KRN_SYNTHETIC_HEADERS PREPEND "${PROJECT_SOURCE_DIR}/")
add_library(krn_plugin_reference STATIC
  ${KRN_SYNTHETIC_SOURCES} ${KRN_SYNTHETIC_HEADERS})
target_include_directories(krn_plugin_reference PUBLIC
  "${PROJECT_SOURCE_DIR}/plugins/reference/include")
target_link_libraries(krn_plugin_reference PUBLIC krn_engine)

# Warnings — apply to every first-party target (ENC-806 / L12). The servers,
# krn_tests and the benchmarks are wired at their definition sites below.
krn_apply_warnings(krn_engine)
krn_apply_warnings(krn_plugin_reference)

if (WIN32)
  add_definitions(-D_WIN32_WINNT=0x0A00 -DNOMINMAX)
endif()

# =======================
# Host + server executables (ENC-1559, purity SPEC D6)
# =======================
# krn_host is the engine composition every server binary shares (src/host/,
# include/krn/host/). It is an engine library target: defined in consumed mode
# too, and exported as krn::krn_host. The executable is top-level only.
# Defined BEFORE the Release block so that block can see them (ENC-1583).
add_library(krn_host STATIC
  "${PROJECT_SOURCE_DIR}/src/host/Host.cpp"
  "${PROJECT_SOURCE_DIR}/include/krn/host/Host.hpp")
add_library(krn::krn_host ALIAS krn_host)
target_link_libraries(krn_host PUBLIC krn_engine)
krn_apply_warnings(krn_host)

if (_krn_top_level)
  # krn_engine_server [wsPort] [conf] — krn_host + the reference plugin.
  add_executable(krn_engine_server "${PROJECT_SOURCE_DIR}/apps/engine_server/main.cpp")
  target_link_libraries(krn_engine_server PRIVATE krn_host krn_plugin_reference)
  krn_apply_warnings(krn_engine_server)
endif()


# =======================
# Release optimizations
# =======================
if (CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  if (MSVC)
    target_compile_options(krn_engine           PRIVATE /O2 /Ob2 /GL)
    target_link_options(krn_engine           PUBLIC /LTCG)
  else()
    # Microarchitecture (ENC-790 / H5). -O3 always; -march is portable by default.
    set(KRN_RELEASE_ARCH_FLAGS "")
    if (KRN_NATIVE_ARCH)
      list(APPEND KRN_RELEASE_ARCH_FLAGS -march=native)
      message(STATUS "Release: -march=native (KRN_NATIVE_ARCH=ON) — binary pinned to THIS CPU")
    elseif (NOT KRN_ARCH_BASELINE STREQUAL "" AND
            CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
      list(APPEND KRN_RELEASE_ARCH_FLAGS "-march=${KRN_ARCH_BASELINE}")
      message(STATUS "Release: portable -march=${KRN_ARCH_BASELINE} (set KRN_NATIVE_ARCH=ON to target the build CPU)")
    else()
      message(STATUS "Release: no -march (portable default)")
    endif()
    target_compile_options(krn_engine           PRIVATE -O3 ${KRN_RELEASE_ARCH_FLAGS})
  endif()

  include(CheckIPOSupported)
  check_ipo_supported(RESULT ipo_supported OUTPUT ipo_output)
  if (ipo_supported)
    set_property(TARGET krn_engine           PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    set(_krn_lto_targets krn_engine)
    foreach (_krn_t IN ITEMS krn_host krn_engine_server)
      if (TARGET ${_krn_t})
        set_property(TARGET ${_krn_t} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
        list(APPEND _krn_lto_targets ${_krn_t})
      endif()
    endforeach()
    list(JOIN _krn_lto_targets " + " _krn_lto_targets)
    message(STATUS "LTO enabled for ${_krn_lto_targets}")
  else()
    message(STATUS "LTO not supported: ${ipo_output}")
  endif()
endif()

# Tests (opt-in)
if (KRN_BUILD_TESTS)
  enable_testing()

  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)  # cmake --install ships krn only
  FetchContent_MakeAvailable(googletest)

  # krn_tests: the engine suite. Its bootstrap registers no connector, and it
  # links krn_engine (ENC-1562; krn_engine_isolation check D enforces it).
  set(KRN_TEST_SOURCES
    tests/connection/ConnectionTest.cpp
    tests/connectors/SyntheticConnectorTest.cpp
    tests/context/ContextTest.cpp
    tests/core/AtomicStoreCapsTest.cpp
    tests/core/AtomicStoreTest.cpp
    tests/core/ConfigNamespaceDispatchTest.cpp
    tests/core/ConfigTest.cpp
    tests/core/ConfigUnclaimedKeysTest.cpp
    tests/core/ExprTest.cpp
    tests/core/JsonUtilNonFiniteTest.cpp
    tests/core/LoggerTest.cpp
    tests/core/RecordValueTest.cpp
    tests/core/SelectFunctionTest.cpp
    tests/core/ThreadPoolTest.cpp
    tests/dispatch/AtomicKeyNamespaceTest.cpp
    tests/dispatch/ClockCoherentPullTest.cpp
    tests/dispatch/DispatchTest.cpp
    tests/dispatch/InjectedAtomicTest.cpp
    tests/dispatch/ListenerMultiplicityTest.cpp
    tests/dispatch/RecordIngressEngineTest.cpp
    tests/engine/ConnectorLifecycleTest.cpp
    tests/engine/EngineRegistriesShapeTest.cpp
    tests/engine/EventComputerCacheTest.cpp
    tests/engine/IngressFactoryDispatchTest.cpp
    tests/engine/RegistriesTest.cpp
    tests/engine/SymbolTableTest.cpp
    tests/feed/GapFixesTest.cpp
    tests/forum/ConnectorsClientTest.cpp
    tests/integration/ConcurrencyContentionTest.cpp
    tests/integration/Enc101CanonicalPatternTest.cpp
    tests/integration/ExpressionLanguageCapstoneTest.cpp
    tests/integration/LetBindingsTest.cpp
    tests/integration/OrderedDeliveryTest.cpp
    tests/integration/StressTest.cpp
    tests/mapping/FunctionMapTest.cpp
    tests/nodes/AggregateTest.cpp
    tests/nodes/AtomicAccessorTest.cpp
    tests/nodes/BucketStartStampTest.cpp
    tests/nodes/BucketTimeTest.cpp
    tests/nodes/ExprNodeTest.cpp
    tests/nodes/FieldTest.cpp
    tests/nodes/FilterTest.cpp
    tests/nodes/GroupSplitTest.cpp
    tests/nodes/IntervalTest.cpp
    tests/nodes/ListenerTest.cpp
    tests/nodes/PackTest.cpp
    tests/nodes/ResponderTest.cpp
    tests/nodes/SwitchTest.cpp
    tests/nodes/TeeTest.cpp
    tests/nodes/TumblingWindowTest.cpp
    tests/nodes/VectorReducerTest.cpp
    tests/nodes/WindowReducerCompositionTest.cpp
    tests/nodes/WorkerTest.cpp
    tests/registry/RegistryTest.cpp
    tests/server/RequestKeyHashTest.cpp
    tests/test_bootstrap.cpp
    tests/treebuilder/ComposedChainTest.cpp
    tests/treebuilder/CorpusTest.cpp
    tests/treebuilder/FanInArityTest.cpp
    tests/treebuilder/FanInJoinKeyTest.cpp
    tests/treebuilder/RecordTerminalTest.cpp
    tests/treebuilder/TreeBuilderTest.cpp
    tests/validation/ValidationTest.cpp
    tests/ws/ClientSessionBackpressureTest.cpp
    tests/ws/ClientSessionTest.cpp
    tests/ws/SubscriptionStrandMintTest.cpp
    tests/ws/WSResponderTest.cpp
    tests/ws/WebSocketE2ETest.cpp
    tests/ws/WsBridgeTest.cpp
  )
  list(TRANSFORM KRN_TEST_SOURCES PREPEND "${PROJECT_SOURCE_DIR}/")
  add_executable(krn_tests ${KRN_TEST_SOURCES})
  target_include_directories(krn_tests PRIVATE "${PROJECT_SOURCE_DIR}/include")
  if (Boost_FOUND)
    target_include_directories(krn_tests PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(krn_tests PRIVATE krn_engine Boost::system)
  else()
    target_link_libraries(krn_tests PRIVATE krn_engine Boost::system)
    target_compile_definitions(krn_tests PRIVATE
      BOOST_ALL_NO_LIB
      BOOST_ERROR_CODE_HEADER_ONLY
      BOOST_SYSTEM_NO_DEPRECATED
      BOOST_ASIO_NO_DEPRECATED
      BOOST_BEAST_USE_STD_STRING_VIEW
    )
  endif()
  target_link_libraries(krn_tests PRIVATE
    Threads::Threads
    RapidJSON::rapidjson
    GTest::gtest_main
    krn_plugin_reference
  )
  krn_apply_warnings(krn_tests)

  # CorpusTest data (ENC-807 / L17): copy the corpus next to the test binary and
  # run the test from that directory, so CorpusTest can always locate
  # corpus_requests.json (it otherwise GTEST_SKIPs / fails on the missing file).
  add_custom_command(TARGET krn_tests POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      "${PROJECT_SOURCE_DIR}/tests/treebuilder/corpus_requests.json"
      "$<TARGET_FILE_DIR:krn_tests>/corpus_requests.json"
    COMMENT "Copying corpus_requests.json next to krn_tests")

  # ENC-1340: let the binary find its corpus without help from the cwd.
  # The three historical search paths were all cwd-relative, so `ctest` (which
  # sets WORKING_DIRECTORY, above) always worked and a hand-run of the raw
  # binary from anywhere else produced a dozen failures that looked like
  # TreeBuilder bugs. tests/support/CorpusPath.hpp adds two cwd-independent
  # candidates: the directory of the running executable, and this one — the
  # configure-time source tree. Between them a binary built by this project
  # finds the corpus from any directory, and the diagnostic it prints when it
  # genuinely cannot names the cwd and every path tried.
  target_compile_definitions(krn_tests PRIVATE
    KRN_TEST_SOURCE_DIR="${PROJECT_SOURCE_DIR}")

  # ─── ENC-1289's EXPECTED-FAILURE MACHINERY IS GONE. ENC-1292 CLEARED THE
  # LAST GATE, 2026-09-20.
  #
  # What stood here: `tests/treebuilder/CorpusTest.cpp`'s value assertions
  # (SPEC specs/2026-09-20-krn-join-correctness D8) were red on `master` BY
  # DESIGN — an instrument that cannot fail is not a gate — so each was
  # registered as its own ctest case with `WILL_FAIL TRUE`, excluded from the
  # main `krn_tests` case by a `--gtest_filter=-...` list, and guarded by
  # `gma_enc1289_expected_failures_really_failed`, which pinned the COUNT and
  # required gtest's own end-of-run summary line so a crash could not read as
  # an expected failure. It unwound itself by construction: each fix flipped
  # its WILL_FAIL case RED and forced whoever landed it to come here.
  #
  # It did exactly that, three times, and the count is the record:
  #   3  ENC-1289 (the instrument lands first, D8)
  #   2  ENC-1005  — the per-request strand; Corpus86_SpreadIsExactlyTwoCents
  #   1  ENC-1291  — port-indexed fan-in; Corpus86_JoinMustNotComplete...
  #   0  ENC-1292  — the declared join key; Corpus87_CrossSymbolJoin...
  #
  # ENC-1291 left the last one deliberately: port-indexing turned "six tuples
  # pairing one symbol with itself" into "no tuples at all", so the test went
  # green VACUOUSLY — every assertion true over an empty tuple list. It added
  # an anti-vacuity ASSERT and kept the marker rather than bank that. ENC-1292
  # declares `by:"none"` on a copy of the corpus entry, the join emits six
  # MIXED tuples, and the assertion is now checking a non-empty set. All three
  # corpus 87 tests — the gate, its observation-point control, and the
  # cross-corpus measurement in `tests/treebuilder/FanInJoinKeyTest.cpp` —
  # now run inside the ordinary `krn_tests` case below, uninverted.
  #
  # NOTE (unchanged and still true): the ENC-1065 `--gtest_repeat` check must
  # be run against the RAW, UNFILTERED binary (`./krn_tests --gtest_repeat=N`).
  # It always had to be; there is simply no filter left for it to dodge.
  add_test(NAME krn_tests
    COMMAND krn_tests
    WORKING_DIRECTORY "$<TARGET_FILE_DIR:krn_tests>")

  # The ws half of ENC-1006's port-conflict test is krn_port_conflict_ws,
  # below; the feed half left with krn_server for krn-common (ENC-1570).
  find_package(Python3 COMPONENTS Interpreter QUIET)
endif()

# ENC-1559: krn_engine_server serves reference.* values over a real WebSocket
# and exits 0 on SIGTERM.
if (KRN_BUILD_TESTS AND TARGET krn_engine_server AND Python3_Interpreter_FOUND)
  add_test(NAME krn_engine_server_reference
    COMMAND "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/tests/scripts/engine_server_reference_test.py"
            "$<TARGET_FILE:krn_engine_server>")
  set_tests_properties(krn_engine_server_reference PROPERTIES TIMEOUT 120)

  add_test(NAME krn_port_conflict_ws
    COMMAND "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/tests/scripts/port_conflict_test.py"
            "$<TARGET_FILE:krn_engine_server>" ws)
  set_tests_properties(krn_port_conflict_ws PROPERTIES TIMEOUT 120)

  # ENC-1595: one WARN per config key nobody claims, and the server still starts.
  add_test(NAME krn_config_unclaimed_keys
    COMMAND "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/tests/scripts/config_unclaimed_keys_test.py"
            "$<TARGET_FILE:krn_engine_server>")
  set_tests_properties(krn_config_unclaimed_keys PROPERTIES TIMEOUT 120)
endif()

# Benchmarks (opt-in)
if (KRN_BUILD_BENCHMARKS)
  include(FetchContent)
  FetchContent_Declare(
    googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.8.3
  )
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googlebenchmark)

  # Helper: create a benchmark executable from a single source file
  function(krn_add_benchmark NAME SRC)
    add_executable(${NAME} ${SRC})
    target_include_directories(${NAME} PRIVATE "${PROJECT_SOURCE_DIR}/include")
    if (Boost_FOUND)
      target_include_directories(${NAME} PRIVATE ${Boost_INCLUDE_DIRS})
      target_link_libraries(${NAME} PRIVATE krn Boost::system)
    else()
      target_link_libraries(${NAME} PRIVATE krn Boost::system)
      target_compile_definitions(${NAME} PRIVATE
        BOOST_ALL_NO_LIB
        BOOST_ERROR_CODE_HEADER_ONLY
        BOOST_SYSTEM_NO_DEPRECATED
        BOOST_ASIO_NO_DEPRECATED
        BOOST_BEAST_USE_STD_STRING_VIEW
      )
    endif()
    target_link_libraries(${NAME} PRIVATE
      Threads::Threads
      RapidJSON::rapidjson
      benchmark::benchmark
    )
    krn_apply_warnings(${NAME})
  endfunction()

  krn_add_benchmark(bench_dispatcher        "${PROJECT_SOURCE_DIR}/benchmarks/DispatcherBench.cpp")
  krn_add_benchmark(bench_atomic_store      "${PROJECT_SOURCE_DIR}/benchmarks/AtomicStoreBench.cpp")
  krn_add_benchmark(bench_thread_pool       "${PROJECT_SOURCE_DIR}/benchmarks/ThreadPoolBench.cpp")
  krn_add_benchmark(bench_window_nodes      "${PROJECT_SOURCE_DIR}/benchmarks/WindowNodesBench.cpp")

  # Convenience target: build all benchmarks at once
  add_custom_target(krn_benchmarks DEPENDS
    bench_dispatcher
    bench_atomic_store
    bench_thread_pool
    bench_window_nodes
  )
endif()

# =======================
# Engine isolation gate (ENC-1481)
# =======================
# THE INVARIANT: krn_engine must not acquire a dependency on Arrow, the
# ClickHouse client, or the krn_data library target (SPEC D14,
# krn-data's docs/data-layer.md §4 rule 6). SPEC D14's live-only client variant omits
# krn_data entirely and must still link, so this is a shipping constraint and
# not a style preference.
#
# It is checked rather than claimed because every way of breaking it still
# compiles and still passes every existing test: one `target_link_libraries`
# line would make the live-only variant impossible and nothing would say so.
#
# ONE ROW PER FIRST-PARTY TARGET, BECAUSE THE CHECK WALKS THE CLOSURE.
# krn_engine_server links krn_host and the reference plugin, so reading only
# its own LINK_LIBRARIES is blind to exactly the line that breaks the
# invariant: `target_link_libraries(krn_host PUBLIC krn_data)`, or `krn::arrow`
# on the plugin. Both leave the root's own property unchanged and both make
# the live-only variant unlinkable. The checker therefore reads a link GRAPH —
# `link.<target>` / `interface_link.<target>` — and walks it transitively.
# Add a first-party library target here when you add one; a target with no row
# is treated as a leaf and only pattern-matched, which is correct for system
# and imported libraries and wrong for ours.
#
# THE CHECKED ROOTS are krn_engine, krn_host, krn_engine_server and krn_tests
# (ENC-1561, purity SPEC D11.2). krn_tests is there because krn-data's
# data/CMakeLists.txt names "the engine test suite is built against an
# Arrow-free include path" as the thing being protected. No target in this
# repo may reach krn_connector_market (D11.1): it and krn_server are
# krn-common's since ENC-1570, so the ban covers every graphed target.
#
# A MISSING ROOT IS A CONFIGURE ERROR under KRN_BUILD_TESTS=ON (D11.2). It
# used to be a warning, and a missing src/main.cpp then dropped ctest from
# seven cases to three, green.
#
# THE POSITIVE CONTROL IS krn_isolation_control (ENC-1507, purity SPEC Q7).
# Its closure must trip a forbidden pattern, or the patterns match nothing in
# this build and a clean verdict about krn_engine is a verdict from a search
# that cannot find anything. It is declared below, in this file, so it exists
# in every configuration the gate runs in, and it is the only control: data/
# and krn_data left for krn-data (ENC-1573). A missing or silent control is
# exit 2 (could not run), never a pass.
#
# THE RESOLVED RAPIDJSON PATH IS IN THE MANIFEST (ENC-1497). `/third_party/`
# is gitignored, so it is populated in the shared checkout and absent in a
# fresh `git worktree`; find_path above therefore resolves RapidJSON to
# ${PROJECT_SOURCE_DIR}/third_party/rapidjson in one and to /usr/include in the
# other. That made ONE COMMIT FAIL THIS GATE FROM ONE CHECKOUT AND PASS FROM
# THE OTHER, with nothing in either run's output to explain the difference.
# Check B now accepts both (bounded -- see the checker's docstring), and the
# path is reported either way, because a verdict that depends on the checkout
# has to print the thing that differs.
#
# The manifest is produced with file(GENERATE) rather than passed on the
# command line because the properties are generator expressions, and because a
# resolved list on an add_test COMMAND would be split into separate arguments
# at every semicolon.
set(_krn_iso_roots krn_engine krn_host krn_engine_server)
set(_krn_iso_missing "")
foreach (_krn_iso_r IN LISTS _krn_iso_roots)
  if (NOT TARGET ${_krn_iso_r})
    list(APPEND _krn_iso_missing ${_krn_iso_r})
  endif()
endforeach()
if (_krn_top_level AND KRN_BUILD_TESTS AND _krn_iso_missing)
  message(FATAL_ERROR
    "krn_engine_isolation cannot run: root target(s) ${_krn_iso_missing} "
    "not defined (roots: ${_krn_iso_roots}). The SPEC D14 / purity SPEC D11 "
    "invariant would not be checked, so this is an error under "
    "KRN_BUILD_TESTS=ON. If a root left the repo on purpose, remove it from "
    "_krn_iso_roots in CMakeLists.txt.")
endif()
if (_krn_top_level AND KRN_BUILD_TESTS)
  set(KRN_ISOLATION_MANIFEST "${PROJECT_BINARY_DIR}/krn_engine_isolation.manifest")

  # The gate's positive control. Scanned, never linked: the checker fails if
  # any other target's closure reaches it. The stub carries no library, so a
  # link to it would compile silently -- which is why that is checked.
  add_library(arrow::arrow_control_probe INTERFACE IMPORTED)
  add_library(krn_isolation_control INTERFACE)
  target_link_libraries(krn_isolation_control
    INTERFACE arrow::arrow_control_probe)

  # THE TARGET LIST IS ENUMERATED FROM CMAKE, NOT HAND-WRITTEN. A hand-written
  # list reproduces the original hole one level deeper: a new first-party
  # library would arrive with no row, be treated by the checker as a leaf, and
  # have its own dependencies never walked — so `krn_store -> krn_data` would
  # pass green exactly the way `krn -> krn_data` used to. BUILDSYSTEM_TARGETS
  # registers a target by its existing. UTILITY targets are dropped because
  # they have no link line. (CMake does not report IMPORTED or ALIAS targets
  # here, which is why the checker ALSO fails on any first-party-looking name
  # that has no row — belt and braces, and the braces are producer-independent.)
  set(_krn_iso_dirs "${PROJECT_SOURCE_DIR}")
  set(_krn_iso_link_targets "")
  foreach (_krn_iso_dir IN LISTS _krn_iso_dirs)
    get_directory_property(_krn_iso_dir_targets
      DIRECTORY "${_krn_iso_dir}" BUILDSYSTEM_TARGETS)
    foreach (_krn_iso_t IN LISTS _krn_iso_dir_targets)
      get_target_property(_krn_iso_type ${_krn_iso_t} TYPE)
      if (NOT _krn_iso_type STREQUAL "UTILITY")
        list(APPEND _krn_iso_link_targets ${_krn_iso_t})
      endif()
    endforeach()
  endforeach()
  list(REMOVE_DUPLICATES _krn_iso_link_targets)
  list(SORT _krn_iso_link_targets)

  set(_krn_iso_link_rows "")
  foreach (_krn_iso_t IN LISTS _krn_iso_link_targets)
    string(APPEND _krn_iso_link_rows
"link.${_krn_iso_t}=$<JOIN:$<TARGET_PROPERTY:${_krn_iso_t},LINK_LIBRARIES>,|>
interface_link.${_krn_iso_t}=$<JOIN:$<TARGET_PROPERTY:${_krn_iso_t},INTERFACE_LINK_LIBRARIES>,|>
")
  endforeach()
  message(STATUS
    "krn_engine_isolation: link graph covers ${_krn_iso_link_targets}")

  file(GENERATE OUTPUT "${KRN_ISOLATION_MANIFEST}" CONTENT
"# Generated by CMake for tools/check_engine_isolation.py (ENC-1481). Do not edit.
root=${PROJECT_SOURCE_DIR}
engine_include_dirs=$<JOIN:$<TARGET_PROPERTY:krn_engine,INCLUDE_DIRECTORIES>,|>
engine_interface_include_dirs=$<JOIN:$<TARGET_PROPERTY:krn_engine,INTERFACE_INCLUDE_DIRECTORIES>,|>
engine_cmake_sources=$<JOIN:$<TARGET_PROPERTY:krn_engine,SOURCES>,|>
host_cmake_sources=$<JOIN:$<TARGET_PROPERTY:krn_host,SOURCES>,|>
engine_server_cmake_sources=$<JOIN:$<TARGET_PROPERTY:krn_engine_server,SOURCES>,|>
rapidjson_include_dir=${RAPIDJSON_INCLUDE_DIR}
${_krn_iso_link_rows}")

  if (Python3_Interpreter_FOUND)
    add_test(NAME krn_engine_isolation
      COMMAND "${Python3_EXECUTABLE}"
              "${PROJECT_SOURCE_DIR}/tools/check_engine_isolation.py"
              --manifest "${KRN_ISOLATION_MANIFEST}")
    set_tests_properties(krn_engine_isolation PROPERTIES TIMEOUT 120)

    # The gate's own gate. A predicate that stopped detecting its violation
    # would turn krn_engine_isolation into a green no-op; this runs each one
    # against an input that violates it and fails unless it is caught. The
    # anti-vacuity rule of krn-data's docs/data-layer.md §5.4, applied to the checker.
    add_test(NAME krn_engine_isolation_selftest
      COMMAND "${Python3_EXECUTABLE}"
              "${PROJECT_SOURCE_DIR}/tools/check_engine_isolation.py"
              --selftest)
    set_tests_properties(krn_engine_isolation_selftest PROPERTIES TIMEOUT 120)
  else()
    # An error, not a warning (ENC-1585): a skip here left ctest green with
    # two fewer cases and the invariant unchecked, the same failure as a
    # missing root above.
    message(FATAL_ERROR
      "krn_engine_isolation cannot run: no Python3 interpreter found "
      "(Python3_EXECUTABLE='${Python3_EXECUTABLE}'). The SPEC D14 / purity "
      "SPEC D11 invariant would not be checked, so this is an error under "
      "KRN_BUILD_TESTS=ON. Install python3 or pass -DPython3_EXECUTABLE=<path>.")
  endif()
endif()

# =======================
# Install / export (ENC-1553, purity SPEC D5)
# =======================
# Top-level only: a FetchContent consumer builds krn_engine from source and
# installs nothing of ours. `find_package(krn CONFIG)` gives krn::krn_engine
# and krn::krn_host.
if (_krn_top_level)
  include(GNUInstallDirs)
  set(KRN_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/krn")
  install(TARGETS krn_engine krn_host EXPORT krnTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/krn"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  install(EXPORT krnTargets NAMESPACE krn::
    DESTINATION "${KRN_INSTALL_CMAKEDIR}")
  configure_file("${PROJECT_SOURCE_DIR}/cmake/krnConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/krnConfig.cmake" @ONLY)
  install(FILES "${PROJECT_BINARY_DIR}/krnConfig.cmake"
    DESTINATION "${KRN_INSTALL_CMAKEDIR}")
endif()

# =======================
# Consumer smoke tests (ENC-1553)
# =======================
# The CMake analogue of wt.mjs's assertLinksResolve. Each drives a separate
# consumer project in a fresh dir under this build tree:
#   fetchcontent — FetchContent of this repo's HEAD from a file:// URL, with the
#                  engine's own options set ON in the consumer's cache; asserts
#                  each library's SOURCES sit under the fetched tree and that no
#                  other engine target exists. It checks the COMMITTED HEAD.
#   install      — cmake --install of this build into an empty prefix, then a
#                  find_package(krn CONFIG) consumer that links krn::krn_engine.
if (KRN_BUILD_TESTS)
  foreach (_krn_smoke IN ITEMS fetchcontent install)
    add_test(NAME krn_consumer_${_krn_smoke}_smoke
      COMMAND "${CMAKE_COMMAND}"
              -DMODE=${_krn_smoke}
              "-DKRN_SOURCE_DIR=${PROJECT_SOURCE_DIR}"
              "-DKRN_BINARY_DIR=${PROJECT_BINARY_DIR}"
              "-DKRN_CONFIG=$<CONFIG>"
              "-DKRN_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
              "-DKRN_GENERATOR=${CMAKE_GENERATOR}"
              "-DKRN_RAPIDJSON_ROOT=${RAPIDJSON_INCLUDE_DIR}"
              -P "${PROJECT_SOURCE_DIR}/tests/consumer/run_consumer_smoke.cmake")
    set_tests_properties(krn_consumer_${_krn_smoke}_smoke PROPERTIES TIMEOUT 900)
  endforeach()
endif()
