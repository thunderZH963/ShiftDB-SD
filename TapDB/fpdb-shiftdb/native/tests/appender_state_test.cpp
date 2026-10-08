#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <iostream>
#include <stdexcept>
using namespace fpdb::tuple;
static int checks = 0;
#define CHECK(x)                                                               \
  do {                                                                         \
    ++checks;                                                                  \
    if (!(x))                                                                  \
      throw std::runtime_error(#x);                                            \
  } while (false)
int main() {
  try {
    // Exercise upstream appenders, including nulls, without consuming source
    // buffers.
    for (auto type : {arrow::int32(), arrow::int64(), arrow::float64(),
                      arrow::date64(), arrow::boolean(), arrow::utf8()}) {
      auto appender = ArrayAppenderBuilder::make(type).value();
      auto nulls = arrow::MakeArrayOfNull(type, 2).ValueOrDie();
      CHECK(appender->appendValue(nulls, 0));
      auto first = appender->snapshot().value();
      CHECK(first->length() == 1 && first->null_count() == 1);
      CHECK(appender->snapshot().value()->Equals(first));
      CHECK(appender->appendValue(nulls, 1));
      auto second = appender->snapshot().value();
      CHECK(second->length() == 2 && second->null_count() == 2);
      CHECK(appender->finalize().value()->Equals(second));
      CHECK(first->length() == 1); // Snapshot owns its buffers.
      CHECK(appender->snapshot().value()->length() == 0);
      CHECK(appender->appendValue(nulls, 0));
      CHECK(appender->finalize().value()->Equals(first));
    }
    arrow::Int64Builder builder;
    CHECK(builder.AppendValues({9, 9, 12}).ok());
    auto values = builder.Finish().ValueOrDie();
    auto a = ArrayAppenderBuilder::make(arrow::int64()).value();
    CHECK(a->appendValue(values, 0));
    auto before = a->snapshot().value();
    CHECK(a->appendValue(values, 1));
    CHECK(a->appendValue(values, 2));
    CHECK(a->finalize().value()->Equals(values));
    CHECK(before->length() == 1);
    std::cout << checks << " native appender checks passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
