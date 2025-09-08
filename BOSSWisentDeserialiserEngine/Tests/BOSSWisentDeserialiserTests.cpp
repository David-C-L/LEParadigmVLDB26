#include <string_view>
#define CATCH_CONFIG_RUNNER
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <fstream>
#include <filesystem>
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
namespace {
std::vector<string>
    librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
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
