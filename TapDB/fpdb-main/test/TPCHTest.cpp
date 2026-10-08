//
// Created by Yifei Yang on 12/1/21.
//

#include <doctest/doctest.h>
#include <fpdb/util/Util.h>
#include <unordered_map>
#include "TestUtil.h"

namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf0.01-distributed" * doctest::skip(SKIP_SUITE)) {

  std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
  int parallelDegreeSSB001 = std::stoi(config["parallel"]);

  TEST_CASE ("tpch-sf0.01-distributed-select" * doctest::skip(false || SKIP_SUITE)) {
    std::cout << "input the path of sql instrs:" << std::endl;
    std::string s;
    ::std::cin >> s;
    REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf0.01/csv/",
                                              {s},
                                              parallelDegreeSSB001,
                                              true));
  }
}

TEST_SUITE ("tpch-sf1-distributed" * doctest::skip(SKIP_SUITE)) {
  
  std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
  int parallelDegreeSSB1 = std::stoi(config["parallel"]);

    TEST_CASE ("tpch-sf1-distributed-select" * doctest::skip(false || SKIP_SUITE)) {
    std::cout << "input the path of sql instrs:" << std::endl;
    std::string s;
    ::std::cin >> s;
    REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf1/csv/",
                                              {s},
                                              parallelDegreeSSB1,
                                              true));
  }
}

TEST_SUITE ("tpch-sf10-distributed" * doctest::skip(SKIP_SUITE)) {

  std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
  int parallelDegreeSSB10 = std::stoi(config["parallel"]);

  TEST_CASE ("tpch-sf10-distributed-select" * doctest::skip(false || SKIP_SUITE)) {
    std::cout << "input the path of sql instrs:" << std::endl;
    std::string s;
    ::std::cin >> s;
    REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                              {s},
                                              parallelDegreeSSB10,
                                              true));
  }
}
TEST_SUITE ("tpch-sf30-distributed" * doctest::skip(SKIP_SUITE)) {

  std::unordered_map<std::string, std::string> config = readConfig("parallelDegree.conf");
  int parallelDegreeSSB30 = std::stoi(config["parallel"]);

  TEST_CASE ("tpch-sf30-distributed-select" * doctest::skip(false || SKIP_SUITE)) {
    std::cout << "input the path of sql instrs:" << std::endl;
    std::string s;
    ::std::cin >> s;
    REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf30/csv/",
                                              {s},
                                              parallelDegreeSSB30,
                                              true));
  }
}

}
