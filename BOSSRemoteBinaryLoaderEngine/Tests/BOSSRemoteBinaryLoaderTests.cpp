#include <string_view>
#define CATCH_CONFIG_RUNNER
#include "../Source/BOSSRemoteBinaryLoaderEngine.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <Serialization.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <numeric>
#include <typeinfo>
#include <variant>
using boss::Expression;
using std::string;
using std::literals::string_literals::
operator""s;                        // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using boss::utilities::operator""_; // NOLINT(misc-unused-using-decls)
                                    // clang-tidy bug
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using boss::expressions::CloneReason;
using boss::expressions::ComplexExpression;
using boss::expressions::generic::get;
using boss::expressions::generic::get_if;
using boss::expressions::generic::holds_alternative;
namespace boss {
using boss::expressions::atoms::Span;
};
using std::int64_t;

using boss::serialization::Argument;
using boss::serialization::ArgumentType;
using boss::serialization::RootExpression;
using boss::serialization::SerializedExpression;
using SerializationExpression = boss::serialization::Expression;

namespace {
std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}

std::vector<int8_t> createByteBufferNamed(int32_t dataSetSize, int64_t sf) {
  std::vector<int64_t> vec1(dataSetSize);
  std::iota(vec1.begin(), vec1.end(), dataSetSize * sf);

  std::vector<int8_t> zeroedInt64_t = {0, 0, 0, 0, 0, 0, 0, 0};

  RootExpression *root =
      SerializedExpression(
          "Table"_("value"_("List"_(boss::Span<int64_t>(vector(vec1))))))
          .extractRoot();
  size_t bufferSize = sizeof(Argument) * root->argumentCount +
                      sizeof(ArgumentType) * root->argumentCount +
                      sizeof(SerializationExpression) * root->expressionCount +
                      root->stringArgumentsFillIndex;

  std::vector<int8_t> bytes;
  bytes.insert(bytes.end(),
               reinterpret_cast<const int8_t *>(&(root->argumentCount)),
               reinterpret_cast<const int8_t *>(&(root->argumentCount)) +
                   sizeof(root->argumentCount));
  bytes.insert(bytes.end(),
               reinterpret_cast<const int8_t *>(&(root->expressionCount)),
               reinterpret_cast<const int8_t *>(&(root->expressionCount)) +
                   sizeof(root->expressionCount));

  bytes.insert(bytes.end(), zeroedInt64_t.begin(), zeroedInt64_t.end());
  bytes.insert(
      bytes.end(),
      reinterpret_cast<const int8_t *>(&(root->stringArgumentsFillIndex)),
      reinterpret_cast<const int8_t *>(&(root->stringArgumentsFillIndex)) +
          sizeof(root->stringArgumentsFillIndex));
  bytes.insert(
      bytes.end(), reinterpret_cast<const int8_t *>(&(root->arguments)),
      reinterpret_cast<const int8_t *>(&(root->arguments)) + bufferSize);

  std::free(root);
  return bytes;
}

std::vector<int8_t> createByteBuffer(int32_t dataSetSize, int64_t sf) {
  std::vector<int64_t> vec1(dataSetSize);
  std::iota(vec1.begin(), vec1.end(), dataSetSize * sf);

  std::vector<int8_t> zeroedInt64_t = {0, 0, 0, 0, 0, 0, 0, 0};

  RootExpression *root =
      SerializedExpression("Table"_("List"_(boss::Span<int64_t>(vector(vec1)))))
          .extractRoot();
  size_t bufferSize = sizeof(Argument) * root->argumentCount +
                      sizeof(ArgumentType) * root->argumentCount +
                      sizeof(SerializationExpression) * root->expressionCount +
                      root->stringArgumentsFillIndex;

  std::vector<int8_t> bytes;
  bytes.insert(bytes.end(),
               reinterpret_cast<const int8_t *>(&(root->argumentCount)),
               reinterpret_cast<const int8_t *>(&(root->argumentCount)) +
                   sizeof(root->argumentCount));
  bytes.insert(bytes.end(),
               reinterpret_cast<const int8_t *>(&(root->expressionCount)),
               reinterpret_cast<const int8_t *>(&(root->expressionCount)) +
                   sizeof(root->expressionCount));
  bytes.insert(bytes.end(), zeroedInt64_t.begin(), zeroedInt64_t.end());
  bytes.insert(
      bytes.end(),
      reinterpret_cast<const int8_t *>(&(root->stringArgumentsFillIndex)),
      reinterpret_cast<const int8_t *>(&(root->stringArgumentsFillIndex)) +
          sizeof(root->stringArgumentsFillIndex));
  bytes.insert(
      bytes.end(), reinterpret_cast<const int8_t *>(&(root->arguments)),
      reinterpret_cast<const int8_t *>(&(root->arguments)) + bufferSize);

  std::free(root);
  return bytes;
}

// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST_CASE("Subspans work correctly", "[spans]") {
  auto input = boss::Span<int64_t>{std::vector<int64_t>{1, 2, 4, 3}};
  auto subrange = std::move(input).subspan(1, 3);
  CHECK(subrange.size() == 3);
  CHECK(subrange[0] == 2);
  CHECK(subrange[1] == 4);
  CHECK(subrange[2] == 3);
  auto subrange2 =
      boss::Span<int64_t>{std::vector<int64_t>{1, 2, 3, 2}}.subspan(2);
  CHECK(subrange2[0] == 3);
  CHECK(subrange2[1] == 2);
}

TEST_CASE("Allocate Whole File", "[basics]") { // NOLINT
  int64_t const lowerBound = 0;
  int64_t const upperBound = 283;
  auto const dataSetSize = 10;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/simpleWisentTable.bin"}};

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto result =
        engine.evaluate("Fetch"_("List"_(lowerBound, upperBound), url));
    auto expected =
        "ByteSequence"_(boss::Span<int8_t>(std::move(vector(bytes))));

    REQUIRE(result == expected);
  }
}

TEST_CASE("Allocate Part File", "[basics]") { // NOLINT
  auto const dataSetSize = 10;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/simpleWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {50, 100}, {24, 128}, {128, 256}, {7, 8}, {1, 274}};

  for (const auto &[lb, ub] : boundPairs) {
    for (const auto &[sf, url] : sfUrlPairs) {

      auto bytes = createByteBuffer(dataSetSize, sf);

      auto [ununsed_1, unused_2, unused_3, spans] =
          std::move(std::get<ComplexExpression>(
                        engine.evaluate("Fetch"_("List"_(lb, ub), url))))
              .decompose();
      auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
      auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

      REQUIRE(resultSpan.size() == expectedSpan.size());

      bool equal = true;
      for (size_t i = lb; i < ub; i++) {
        equal &= resultSpan.at(i) == expectedSpan.at(i);
        if (!equal) {
          break;
        }
      }
      REQUIRE(equal == true);
    }
  }
}

TEST_CASE("Allocate Multiple Blocks", "[basics]") { // NOLINT
  auto const dataSetSize = 10;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/simpleWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {50, 100}, {128, 256}, {16, 24}};

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                      "List"_((int64_t)16, (int64_t)24, (int64_t)50,
                              (int64_t)100, (int64_t)128, (int64_t)256),
                      url))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool equal = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        equal &= resultSpan.at(i) == expectedSpan.at(i);
      }
    }
    REQUIRE(equal == true);
  }
}

TEST_CASE("Allocate Multiple Overlapping Blocks", "[basics]") { // NOLINT
  auto const dataSetSize = 10;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/simpleWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {50, 100}, {128, 256}, {7, 8}, {64, 164}};

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(
            std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                "List"_((int64_t)7, (int64_t)8, (int64_t)50, (int64_t)100,
                        (int64_t)128, (int64_t)256, (int64_t)64, (int64_t)164),
                url))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool equal = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        equal &= resultSpan.at(i) == expectedSpan.at(i);
      }
    }
    REQUIRE(equal == true);
  }
}

TEST_CASE("156KB File Test", "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {{77000, 82000},
                                                         {110000, 150000}};

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate(
                      "Fetch"_("List"_((int64_t)77000, (int64_t)82000,
                                       (int64_t)110000, (int64_t)150000),
                               url))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool equal = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        equal &= resultSpan.at(i) == expectedSpan.at(i);
        if (!equal) {
          break;
        }
      }
    }
    REQUIRE(equal == true);
  }
}

TEST_CASE("156KB File Test With Bound Padding", "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {512, 1024}, {2048, 4096}};
  int64_t boundPadding = 4096;

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)512, (int64_t)1024,
                              (int64_t)2048, (int64_t)4096),
                      url, boundPadding))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    bool inPaddingEqual = true;
    int64_t lb = boundPairs[0].first;
    int64_t ub = boundPairs[0].second + boundPadding;
    for (size_t i = lb; i < ub; i++) {
      inPaddingEqual &= resultSpan.at(i) == expectedSpan.at(i);
      if (!inPaddingEqual) {
        break;
      }
    }
    REQUIRE(inPaddingEqual == true);
  }
}

TEST_CASE("156KB File Test With Alignment", "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {512, 1024}, {2048, 4000}};
  int64_t boundPadding = 0;
  int64_t alignment = 4096;

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)512, (int64_t)1024,
                              (int64_t)2048, (int64_t)4000),
                      url, boundPadding, alignment))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    bool inAlignmentEqual = true;
    int64_t lb = boundPairs[0].first - (boundPairs[0].first % alignment);
    int64_t ub =
        boundPairs[0].second + (alignment - (boundPairs[0].second % alignment));
    for (size_t i = lb; i < ub; i++) {
      inAlignmentEqual &= resultSpan.at(i) == expectedSpan.at(i);
      if (!inAlignmentEqual) {
        break;
      }
    }
    REQUIRE(inAlignmentEqual == true);
  }
}

TEST_CASE("156KB File Test With Padding & Alignment", "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {512, 1024}, {2048, 4000}};
  int64_t boundPadding = 0;
  int64_t alignment = 32;

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)512, (int64_t)1024,
                              (int64_t)2048, (int64_t)4000),
                      url, boundPadding, alignment))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    bool inPaddingEqual = true;
    int64_t lb = boundPairs[0].first;
    int64_t ub = boundPairs[0].second + boundPadding;
    for (size_t i = lb; i < ub; i++) {
      inPaddingEqual &= resultSpan.at(i) == expectedSpan.at(i);
      if (!inPaddingEqual) {
        break;
      }
    }
    REQUIRE(inPaddingEqual == true);

    bool inAlignmentEqual = true;
    lb = boundPairs[0].first - (boundPairs[0].first % alignment);
    ub = boundPairs[0].second + boundPadding +
         (alignment - ((boundPairs[0].second + boundPadding) % alignment));
    for (size_t i = lb; i < ub; i++) {
      inAlignmentEqual &= resultSpan.at(i) == expectedSpan.at(i);
      if (!inAlignmentEqual) {
        break;
      }
    }
    REQUIRE(inAlignmentEqual == true);
  }
}

TEST_CASE("156KB File Test With Padding & Alignment & Multipart",
          "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {77825, 81919}};
  int64_t boundPadding = 0;
  int64_t alignment = 4096;
  int64_t maxRanges = -1;

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)77825, (int64_t)81919),
                      url, boundPadding, alignment, maxRanges))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    for (int64_t j = 0; j < boundPairs.size(); j++) {
      bool inPaddingEqual = true;
      int64_t lb = boundPairs[j].first;
      int64_t ub = boundPairs[j].second + boundPadding;
      for (size_t i = lb; i < ub; i++) {
        inPaddingEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inPaddingEqual) {
          break;
        }
      }
      REQUIRE(inPaddingEqual == true);

      bool inAlignmentEqual = true;
      lb = boundPairs[j].first - (boundPairs[j].first % alignment);
      ub = boundPairs[j].second + boundPadding +
           (alignment - ((boundPairs[j].second + boundPadding) % alignment));
      for (size_t i = lb; i < ub; i++) {
        inAlignmentEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inAlignmentEqual) {
          break;
        }
      }
      REQUIRE(inAlignmentEqual == true);
    }
  }
}

TEST_CASE("156KB File Test With Padding & Alignment & Multipart & Span bounds",
          "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<int64_t> boundPairsSpan = {32, 64, 128, 256, 77825, 81919};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {77825, 81919}};
  int64_t boundPadding = 0;
  int64_t alignment = 4096;
  int64_t maxRanges = -1;

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(
            std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                "List"_(boss::Span<int64_t>(std::move(vector(boundPairsSpan)))),
                url, boundPadding, alignment, maxRanges))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    for (int64_t j = 0; j < boundPairs.size(); j++) {
      bool inPaddingEqual = true;
      int64_t lb = boundPairs[j].first;
      int64_t ub = boundPairs[j].second + boundPadding;
      for (size_t i = lb; i < ub; i++) {
        inPaddingEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inPaddingEqual) {
          break;
        }
      }
      REQUIRE(inPaddingEqual == true);

      bool inAlignmentEqual = true;
      lb = boundPairs[j].first - (boundPairs[j].first % alignment);
      ub = boundPairs[j].second + boundPadding +
           (alignment - ((boundPairs[j].second + boundPadding) % alignment));
      for (size_t i = lb; i < ub; i++) {
        inAlignmentEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inAlignmentEqual) {
          break;
        }
      }
      REQUIRE(inAlignmentEqual == true);
    }
  }
}

TEST_CASE("Multipart & Singlepart",
          "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine multiEngine;
  boss::engines::RBL::Engine singleEngine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {77824, 81920}};
  int64_t boundPadding = 0;
  int64_t alignment = 4096;
  int64_t maxRanges = -1;

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [multiUnunsed_1, multiUnused_2, multiUnused_3, multiSpans] =
        std::move(std::get<ComplexExpression>(multiEngine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)77824, (int64_t)81920),
                      url, boundPadding, alignment, maxRanges))))
            .decompose();
    auto [singleUnunsed_1, singleUnused_2, singleUnused_3, singleSpans] =
        std::move(std::get<ComplexExpression>(singleEngine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)77824, (int64_t)81920),
                      url, boundPadding, alignment))))
            .decompose();
    auto &multiResultSpan = std::get<boss::Span<int8_t>>(multiSpans.at(0));
    auto &singleResultSpan = std::get<boss::Span<int8_t>>(singleSpans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(multiResultSpan.size() == expectedSpan.size());
    REQUIRE(singleResultSpan.size() == expectedSpan.size());
    REQUIRE(multiResultSpan.size() == singleResultSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= multiResultSpan.at(i) == expectedSpan.at(i);
        inBoundsEqual &= singleResultSpan.at(i) == expectedSpan.at(i);
        inBoundsEqual &= multiResultSpan.at(i) == singleResultSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    for (int64_t j = 0; j < boundPairs.size(); j++) {
      bool inPaddingEqual = true;
      int64_t lb = boundPairs[j].first;
      int64_t ub = boundPairs[j].second + boundPadding;
      for (size_t i = lb; i < ub; i++) {
        inPaddingEqual &= multiResultSpan.at(i) == expectedSpan.at(i);
        inPaddingEqual &= singleResultSpan.at(i) == expectedSpan.at(i);
        inPaddingEqual &= multiResultSpan.at(i) == singleResultSpan.at(i);
        if (!inPaddingEqual) {
          break;
        }
      }
      REQUIRE(inPaddingEqual == true);

      bool inAlignmentEqual = true;
      lb = boundPairs[j].first - (boundPairs[j].first % alignment);
      ub = boundPairs[j].second + boundPadding +
           (alignment - ((boundPairs[j].second + boundPadding) % alignment));
      for (size_t i = lb; i < ub; i++) {
        inAlignmentEqual &= multiResultSpan.at(i) == expectedSpan.at(i);
        inAlignmentEqual &= singleResultSpan.at(i) == expectedSpan.at(i);
        inAlignmentEqual &= multiResultSpan.at(i) == singleResultSpan.at(i);
        if (!inAlignmentEqual) {
          break;
        }
      }
      REQUIRE(inAlignmentEqual == true);
    }
  }
}

TEST_CASE("156KB File Test With Padding & Alignment with Overhead Tracking",
          "[basics]") { // NOLINT
  auto const dataSetSize = 10000;
  boss::engines::RBL::Engine engine;
  std::vector<std::pair<int64_t, std::string>> sfUrlPairs = {
      {1, "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin"}};
  std::vector<std::pair<int64_t, int64_t>> boundPairs = {
      {32, 64}, {128, 256}, {512, 1024}, {2048, 4000}};
  int64_t boundPadding = 4096;
  int64_t alignment = 4096;

  engine.evaluate("StartTrackingOverhead"_());

  for (const auto &[sf, url] : sfUrlPairs) {

    auto bytes = createByteBuffer(dataSetSize, sf);

    auto [ununsed_1, unused_2, unused_3, spans] =
        std::move(std::get<ComplexExpression>(engine.evaluate("Fetch"_(
                      "List"_((int64_t)32, (int64_t)64, (int64_t)128,
                              (int64_t)256, (int64_t)512, (int64_t)1024,
                              (int64_t)2048, (int64_t)4000),
                      url, boundPadding, alignment))))
            .decompose();
    auto &resultSpan = std::get<boss::Span<int8_t>>(spans.at(0));
    auto expectedSpan = boss::Span<int8_t>(std::move(vector(bytes)));

    REQUIRE(resultSpan.size() == expectedSpan.size());

    bool inBoundsEqual = true;
    for (const auto &[lb, ub] : boundPairs) {
      for (size_t i = lb; i < ub; i++) {
        inBoundsEqual &= resultSpan.at(i) == expectedSpan.at(i);
        if (!inBoundsEqual) {
          break;
        }
      }
    }
    REQUIRE(inBoundsEqual == true);

    bool inPaddingEqual = true;
    int64_t lb = boundPairs[0].first;
    int64_t ub = boundPairs[0].second + boundPadding;
    for (size_t i = lb; i < ub; i++) {
      inPaddingEqual &= resultSpan.at(i) == expectedSpan.at(i);
      if (!inPaddingEqual) {
        break;
      }
    }
    REQUIRE(inPaddingEqual == true);

    bool inAlignmentEqual = true;
    lb = boundPairs[0].first - (boundPairs[0].first % alignment);
    ub = boundPairs[0].second + boundPadding +
         (alignment - ((boundPairs[0].second + boundPadding) % alignment));
    for (size_t i = lb; i < ub; i++) {
      inAlignmentEqual &= resultSpan.at(i) == expectedSpan.at(i);
      if (!inAlignmentEqual) {
        break;
      }
    }
    REQUIRE(inAlignmentEqual == true);
  }

  engine.evaluate("StopTrackingOverhead"_());
}

TEST_CASE("Allocate Multiple Whole Files", "[basics]") { // NOLINT
  boss::engines::RBL::Engine engine;
  auto const dataSetSize1 = 10;
  auto const dataSetSize2 = 10000;
  auto const url1 = "https://www.doc.ic.ac.uk/~dcl19/simpleWisentTable.bin";
  auto const url2 = "https://www.doc.ic.ac.uk/~dcl19/largeWisentTable.bin";
  auto bytes1 = createByteBuffer(dataSetSize1, 1);
  auto bytes2 = createByteBuffer(dataSetSize2, 1);

  auto result = engine.evaluate("Fetch"_(url1, url2));
  auto expected = "ByteSequences"_(
      "ByteSequence"_(boss::Span<int8_t>(std::move(vector(bytes1)))),
      "ByteSequence"_(boss::Span<int8_t>(std::move(vector(bytes2)))));

  REQUIRE(result == expected);
}

int main(int argc, char *argv[]) {
  Catch::Session session;
  session.cli(session.cli() |
              Catch::clara::Opt(librariesToTest, "library")["--library"]);
  auto const returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    return returnCode;
  }
  return session.run();
}
