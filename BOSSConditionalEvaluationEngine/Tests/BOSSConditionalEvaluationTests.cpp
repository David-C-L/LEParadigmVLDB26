#include <string_view>
#define CATCH_CONFIG_RUNNER
#include "../Source/BOSSConditionalEvaluationEngine.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <Serialization.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <fstream>
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
using SerializationExpression = boss::serialization::Expression;

namespace {
std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}
// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST_CASE("Subspans work correctly", "[spans]") { // NOLINT
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

TEST_CASE("Set expressions work correctly", "[sets]") { // NOLINT
  boss::engines::ConditionalEvaluation::Engine engine;
  auto resultSet1 = engine.evaluate("Set"_("x"_, 1));
  auto resultSet2 = engine.evaluate("x"_);
  auto resultSet3 = engine.evaluate("Set"_("x"_, "Increment"_("x"_)));
  auto resultSet4 = engine.evaluate("x"_);

  auto resultBoolSet1 = engine.evaluate("Set"_("x"_, true));
  auto resultBoolSet2 = engine.evaluate("x"_);

  CHECK(resultSet1 == 1);
  CHECK(resultSet2 == 1);
  CHECK(resultSet3 == (int64_t)2);
  CHECK(resultSet4 == (int64_t)2);

  CHECK(resultBoolSet1 == true);
  CHECK(resultBoolSet2 == true);
}

TEST_CASE("EvaluateIf expressions work correctly", "[evaluateifs]") { // NOLINT
  boss::engines::ConditionalEvaluation::Engine engine;
  auto resultNoEvaluate = engine.evaluate("EvaluateIf"_(false, ("Increment"_(1))));
  auto resultEvaluate = engine.evaluate("EvaluateIf"_(true, ("Increment"_(1))));
  CHECK(resultEvaluate == (int64_t) 2);
  CHECK(resultNoEvaluate == "EvaluateIf"_(false, ("__Increment"_(1))));

  auto nestedResultNoEvaluate = engine.evaluate("EvaluateIf"_(false, ("ThisIsHeld"_("Increment"_(1)))));
  auto nestedResultEvaluate = engine.evaluate("EvaluateIf"_(true, ("ThisIsNotHeld"_("Increment"_(1)))));
  CHECK(nestedResultEvaluate == "ThisIsNotHeld"_((int64_t) 2));
  CHECK(nestedResultNoEvaluate == "EvaluateIf"_(false, ("__ThisIsHeld"_((int64_t) 2))));
  
  auto outerNestedResultNoEvaluate = engine.evaluate("Stays"_("EvaluateIf"_(false, ("ThisIsHeld"_("Increment"_(1))))));
  auto outerNestedResultEvaluate = engine.evaluate("Stays"_("EvaluateIf"_(true, ("ThisIsNotHeld"_("Increment"_(1))))));
  CHECK(outerNestedResultEvaluate == "Stays"_("ThisIsNotHeld"_((int64_t) 2)));
  CHECK(outerNestedResultNoEvaluate == "Stays"_("EvaluateIf"_(false, ("__ThisIsHeld"_((int64_t) 2)))));
}

TEST_CASE("EvaluateIf expressions work with int Equals expressions",
          "[evaluateifs][equals]") { // NOLINT
  boss::engines::ConditionalEvaluation::Engine engine;
  auto resultNoEvaluate = engine.evaluate("EvaluateIf"_("Equals"_(1,0), ("Increment"_(1))));
  auto resultEvaluate = engine.evaluate("EvaluateIf"_("Equals"_(0,0), ("Increment"_(1))));
  CHECK(resultEvaluate == (int64_t) 2);
  CHECK(resultNoEvaluate == "EvaluateIf"_("Equals"_(1,0), ("__Increment"_(1))));

  auto nestedResultNoEvaluate = engine.evaluate("EvaluateIf"_("Equals"_(1,0), ("ThisIsHeld"_("Increment"_(1)))));
  auto nestedResultEvaluate = engine.evaluate("EvaluateIf"_("Equals"_(0,0), ("ThisIsNotHeld"_("Increment"_(1)))));
  CHECK(nestedResultEvaluate == "ThisIsNotHeld"_((int64_t) 2));
  CHECK(nestedResultNoEvaluate == "EvaluateIf"_("Equals"_(1,0), ("__ThisIsHeld"_((int64_t) 2))));
  
  auto outerNestedResultNoEvaluate = engine.evaluate("Stays"_("EvaluateIf"_("Equals"_(1,0), ("ThisIsHeld"_("Increment"_(1))))));
  auto outerNestedResultEvaluate = engine.evaluate("Stays"_("EvaluateIf"_("Equals"_(0,0), ("ThisIsNotHeld"_("Increment"_(1))))));
  CHECK(outerNestedResultEvaluate == "Stays"_("ThisIsNotHeld"_((int64_t) 2)));
  CHECK(outerNestedResultNoEvaluate == "Stays"_("EvaluateIf"_("Equals"_(1,0), ("__ThisIsHeld"_((int64_t) 2)))));
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
