#include <doctest/doctest.h>
#include <fpdb/util/Util.h>
#include <unordered_map>
#include "TestUtil.h"

namespace fpdb::main::test {

#define SKIP_SUITE true

TEST_SUITE ("ssb-sf1-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf1-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}

TEST_SUITE ("ssb-sf10-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf10-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf10-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}


TEST_SUITE ("ssb-sf20-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf20-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf20-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}

TEST_SUITE ("ssb-sf30-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf30-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf30-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}


TEST_SUITE ("ssb-sf50-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf50-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf50-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}
TEST_SUITE ("ssb-sf100-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf100-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf100-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}

TEST_SUITE ("ssb-sf150-distributed" * doctest::skip(SKIP_SUITE)) {

std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
int parallelDegreeSSB = std::stoi(config["parallel"]);

TEST_CASE ("ssb-sf150-distributed-original-select" * doctest::skip(false || SKIP_SUITE)) {
  cout << "input the path of sql instrs:" << endl;
  string s;
  ::std::cin >> s;
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf150-sortlineorder/csv/",
                                            {s},
                                            parallelDegreeSSB,
                                            true));
}
}
}

