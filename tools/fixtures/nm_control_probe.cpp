// Check F's positive control (ENC-1491).  Compiled into
// krn_isolation_nm_control, a static library that is SCANNED WITH nm AND NEVER
// LINKED: tools/check_engine_isolation.py fails if any target's link closure
// reaches it.  It is built with krn_engine's INTERPROCEDURAL_OPTIMIZATION, so
// it has the same object format as the archive it vouches for, and a nm that
// cannot read that format fails here as well as there.
//
// Each symbol named krn_isolation_nm_probe must trip the forbidden-symbol rule
// noted beside it, and between them they must trip every rule.  A symbol named
// krn_isolation_nm_probe_decoy must trip none: it is the over-match direction.
// Declarations only, apart from the probes: nothing here needs a header.

namespace arrow { struct Status; Status* krn_isolation_nm_probe(); }      // arrow::
extern "C" int arrow_krn_isolation_nm_probe() { return 1; }               // arrow_
namespace parquet { int krn_isolation_nm_probe() { return 2; } }          // parquet::
namespace gandiva { int krn_isolation_nm_probe() { return 3; } }          // gandiva::
namespace clickhouse { int krn_isolation_nm_probe() { return 4; } }       // clickhouse::
namespace duckdb { int krn_isolation_nm_probe() { return 5; } }           // duckdb::
extern "C" int duckdb_krn_isolation_nm_probe() { return 6; }              // duckdb_

namespace arrow { Status* krn_isolation_nm_probe() { return nullptr; } }

// krn_data: a krn::data name the engine's include/ does not declare.
namespace krn::data {
struct TickRecord;
int krn_isolation_nm_probe(const TickRecord*) { return 7; }
}

// The market connector's namespace, and a derived market type under krn::.
namespace krn::market { int krn_isolation_nm_probe() { return 8; } }
namespace krn::nm_probe {
struct OrderBookManager;
int krn_isolation_nm_probe(const OrderBookManager*) { return 9; }
struct sma_20 {};                                // a prefix word: sma_ -> sma_20
int krn_isolation_nm_probe(sma_20) { return 10; }
}

// Decoys.  Each is close to a rule and must not trip it.
namespace krn::data { class SymbolTable; }  // the engine's own krn::data type
int krn_isolation_nm_probe_decoy(const krn::data::SymbolTable*) { return 11; }
namespace myarrow { int krn_isolation_nm_probe_decoy() { return 12; } }
namespace krn::arrowlike { int krn_isolation_nm_probe_decoy() { return 13; } }
extern "C" int garrow_krn_isolation_nm_probe_decoy() { return 14; }
namespace third_party_probe { struct OrderBook; int krn_isolation_nm_probe_decoy(const OrderBook*) { return 15; } }
namespace krn::nm_probe { int krn_isolation_nm_probe_decoy(double mean) { return static_cast<int>(mean); } }
namespace krn::nm_probe { struct mean {}; int krn_isolation_nm_probe_decoy(mean) { return 16; } }
