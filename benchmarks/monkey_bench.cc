#include <iostream>
#include <string>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/random.h"
#include "util/testutil.h"
#include "util/histogram.h"
#include "leveldb/filter_policy.h"

using std::cout;
using std::endl;

std::string FLAGS_db;

// graphs A-E, 1-5
static int FLAGS_graph_num = 1;

// x-axis argument for graph, have different meanings for each graphs
static int FLAGS_arg = 1;

static bool FLAGS_use_monkey = false;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

leveldb::Env* g_env = nullptr;

namespace leveldb {

// helper class, taken from db_bench.cc
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

// helper class, taken from db_bench.cc
class KeyBuffer {
 public:
  KeyBuffer(int prefix)
    : prefix_(prefix) {
    assert(prefix < sizeof(buffer_));
    memset(buffer_, 'a', prefix);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;
  KeyBuffer(KeyBuffer& other) = delete;

  void Set(int k) {
    std::snprintf(buffer_ + prefix_,
                  sizeof(buffer_) - prefix_, "%016d", k);
  }

  Slice slice() const { return Slice(buffer_, prefix_ + 16); }

 private:
  int prefix_;
  char buffer_[1024];
};

class Benchmark {
private:
  DB* db_;
  Options options_;
public:
  Benchmark()
    : db_(nullptr) {
  }

  ~Benchmark() {
    delete db_;

    for (int i = 0; i < NUM_LEVELS; ++i) {
      if (options_.monkey_filter_policies[i]) {
        delete options_.monkey_filter_policies[i];
      }
    }
    delete options_.filter_policy;
  }

  void RunDefault() {
    // this is the default setup described in the Monkey paper
    //
    // - insert 1GB of key-value entries, with each entry of 1KB
    // and where entries are uniformly distributed across the key space
    // - issue 16KB zero-result point lookups with also keys that are
    // uniformly distributed

    //int total_size = 1024 * 1024 * 1024;
    int total_size = 128 * 1024 * 1024;
    int entry_size = 1024 - 16;
    int num_entries_writes = total_size / entry_size;
    int num_entries_lookup = 16 * 1024;

    Random rand(0);
    RandomGenerator gen;
    WriteOptions write_options;
    ReadOptions read_options;
    KeyBuffer key0(0);
    KeyBuffer key1(1);

    for (int i = 0; i < num_entries_writes; ++i) {
      key0.Set(rand.Uniform(num_entries_writes));
      db_->Put(write_options, key0.slice(), gen.Generate(entry_size));
    }

    std::string value;

    db_->GetProperty("leveldb.stats", &value);
    std::cout << value << std::endl;
    
    double total_time = 0;
    for (int i = 0; i < num_entries_lookup; ++i) {
      key1.Set(rand.Uniform(num_entries_lookup));
      double start = g_env->NowMicros();
      db_->Get(read_options, key1.slice(), &value);
      double end = g_env->NowMicros();
      total_time += (end - start);
    }

    std::cout << "non-hist median " << (total_time / (double) num_entries_lookup) << std::endl;
  }

  void RunBenchmark() {
    std::fprintf(stdout, "nothing to do\n");
  }

  void SetupLeveling(Options& options) {
    options.use_leveled_merge = true;
    size_t size = options.max_file_size * options.size_ratio; // level 1 size
    for (int i = 1; i < NUM_LEVELS; ++i) {
      options.leveled_file_sizes[i] = size;
      size *= options.size_ratio;
    }
  }

  void SetupMonkey(Options& options) {
    options.monkey_filter_policies[0] = NewBloomFilterPolicy(11);
    options.monkey_filter_policies[1] = NewBloomFilterPolicy(9);
    options.monkey_filter_policies[2] = NewBloomFilterPolicy(8);
    options.monkey_filter_policies[3] = NewBloomFilterPolicy(7);
    options.monkey_filter_policies[4] = NewBloomFilterPolicy(5);
    options.monkey_filter_policies[5] = NewBloomFilterPolicy(4);
    options.monkey_filter_policies[6] = NewBloomFilterPolicy(2);
  }

  void Run() {
    bool use_monkey = true;

    DestroyDB(FLAGS_db, Options());
    
    options_.create_if_missing = true;
    options_.env = g_env;
  
    SetupLeveling(options_);
    options_.monkey = use_monkey;
    SetupMonkey(options_);
    options_.filter_policy = NewBloomFilterPolicy(5);

    Status s = DB::Open(options_, FLAGS_db, &db_);
    if (!s.ok()) {
      std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      std::exit(1);
    }

    switch (FLAGS_graph_num) {
      case 0:
        RunDefault();
        break;
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
        RunBenchmark();
        break;
      default:
        std::fprintf(stderr, "Invalid graph number %d\n", FLAGS_graph_num);
        std::exit(1);
        break;
    }
  }
};

}; // namespace leveldb

int main(int argc, char** argv) {
  std::string default_db_path;

  for (int i = 1; i < argc; ++i) {
    int n;
    char junk;

    if (sscanf(argv[i], "-g=%d%c", &n, &junk) == 1) {
      FLAGS_graph_num = n;
    } else if (sscanf(argv[i], "-a=%d%c", &n, &junk) == 1) {
      FLAGS_arg = n;
    } else if (strncmp(argv[i], "-m", 2) == 0) {
      FLAGS_use_monkey = true;
    } else {
      std::fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      std::exit(1);
    }
  }

  g_env = leveldb::Env::Default();
  g_env->GetTestDirectory(&FLAGS_db);
  FLAGS_db += "/monkey";

  leveldb::Benchmark b;
  b.Run();

  return 0;
}
