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

// these parameters need to be tuned for each run
int default_filter_bits = 1;

/*
static const int monkey_filter_bits[] = {
  20, 18, 17, 15,
  14, 12, 11,  9,
   8,  6,  5,  4, 
  19,  1,  1,  1,
   1,  1,  1,  1,
};
*/

static const int monkey_filter_bits[] = {
  40, 35, 30, 25,
  20, 15, 14,  13,
   12,  11,  10,  9, 
  19,  1,  1,  1,
   1,  1,  1,  1,
};


int entry_size = 1024 - 16;
int num_entries_writes = 32768;
int num_entries_lookup = 16 * 1024;

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
  KeyBuffer(char c)
    : prefix_(1) {
    assert(prefix < sizeof(buffer_));
    memset(buffer_, c, prefix_);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;
  KeyBuffer(KeyBuffer& other) = delete;

  void Set(int k) {
    std::snprintf(buffer_ + prefix_,
                  sizeof(buffer_) - prefix_, "%015d", k);
  }

  Slice slice() const { return Slice(buffer_, prefix_ + 15); }

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

  void doRead0(int num_entries) {
    Random rand(time(NULL));
    std::string value;
    ReadOptions read_options;
    KeyBuffer key1('b');
    Histogram hist;

    read_options.fill_cache = false;

    hist.Clear();
    for (int n = 0; n < 3; ++n) {
      double start = g_env->NowMicros();
      for (int i = 0; i < num_entries_lookup; ++i) {
        key1.Set(rand.Next());
        db_->Get(read_options, key1.slice(), &value);

      }
      double end = g_env->NowMicros();
      double taken = (end - start);
      hist.Add(taken);
    }

    std::cout << num_entries << " " << hist.Average() << " " << hist.StandardDeviation() << std::endl;
  }

  void VaryNumEntries() {
    Random rand(time(NULL));
    RandomGenerator gen;
    WriteOptions write_options;
    KeyBuffer key0('a');
    
    int num_entries_start = 1 << 15;
    //int num_entries_end = 1 << 23;
    int num_entries_end = 1 << 19;
    
    for (int i = num_entries_start; i < num_entries_end; ++i) {
      //key0.Set(rand.Next());
      key0.Set(i);
      Status s = db_->Put(write_options, key0.slice(), gen.Generate(entry_size));
      if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        break;
      }

      if (i >= num_entries_start) {
        std::string prop;
        db_->GetProperty("leveldb.stats", &prop);
        std::cout << prop << std::endl;

        doRead0(i);
        num_entries_start *= 2;
      }
    }
  }

  void VaryEntrySize() {
    Random rand(time(NULL));
    RandomGenerator gen;
    WriteOptions write_options;
    KeyBuffer key0('a');

    
    entry_size = FLAGS_arg - 16;
    num_entries_writes = 1024 * 1024;
    
    for (int i = 0; i < num_entries_writes; ++i) {
      //key0.Set(rand.Next());
      key0.Set(i);
      Status s = db_->Put(write_options, key0.slice(), gen.Generate(entry_size));
      if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        break;
      }

    }

    std::string prop;
    db_->GetProperty("leveldb.stats", &prop);
    std::cout << prop << std::endl;

    doRead0(0);
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
    for (int i = 0; i < NUM_LEVELS; ++i) {
      options.monkey_filter_policies[i] = NewBloomFilterPolicy(monkey_filter_bits[i]);
    }
  }

  void Run() {
    DestroyDB(FLAGS_db, Options());
    
    options_.create_if_missing = true;
    options_.env = g_env;
  
    SetupLeveling(options_);
    options_.monkey = FLAGS_use_monkey;
    SetupMonkey(options_);
    options_.filter_policy = NewBloomFilterPolicy(default_filter_bits);
    options_.compression = kNoCompression;

    Status s = DB::Open(options_, FLAGS_db, &db_);
    if (!s.ok()) {
      std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      std::exit(1);
    }

    switch (FLAGS_graph_num) {
      case 0:
        VaryNumEntries();
        break;
      case 1:
        VaryEntrySize();
        break;
      case 2:
        VaryEntrySize();
          break;
      case 3:
        VaryEntrySize();
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
