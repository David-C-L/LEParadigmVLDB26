#include "BOSSDictionaryEncoderEngine.hpp"
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <mutex>

// #define DEBUG

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

static boss::ComplexExpression shallowCopy(boss::ComplexExpression const& e) {
  auto const& head = e.getHead();
  auto const& dynamics = e.getDynamicArguments();
  auto const& spans = e.getSpanArguments();
  boss::ExpressionArguments dynamicsCopy;
  std::transform(dynamics.begin(), dynamics.end(), std::back_inserter(dynamicsCopy),
                 [](auto const& arg) {
                   return std::visit(
                       boss::utilities::overload(
                           [&](boss::ComplexExpression const& expr) -> boss::Expression {
                             return shallowCopy(expr);
                           },
                           [](auto const& otherTypes) -> boss::Expression { return otherTypes; }),
                       arg);
                 });
  boss::expressions::ExpressionSpanArguments spansCopy;
  std::transform(spans.begin(), spans.end(), std::back_inserter(spansCopy), [](auto const& span) {
    return std::visit(
        [](auto const& typedSpan) -> boss::expressions::ExpressionSpanArgument {
          using SpanType = std::decay_t<decltype(typedSpan)>;
          using T = std::remove_const_t<typename SpanType::element_type>;
          if constexpr(std::is_same_v<T, bool>) {
            return SpanType(typedSpan.begin(), typedSpan.size(), []() {});
          } else {
            auto* ptr = const_cast<T*>(typedSpan.begin()); // NOLINT
            return boss::Span<T>(ptr, typedSpan.size(), []() {});
          }
        },
        span);
  });
  return boss::ComplexExpression(head, {}, std::move(dynamicsCopy), std::move(spansCopy));
}

namespace boss::engines::DictionaryEncoder {
namespace utilities {} // namespace utilities

boss::Expression Engine::encodeColumn(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

            auto tempHead = std::move(head);
	    auto saveHead = tempHead;

            std::transform(
                std::make_move_iterator(dynamics.begin()),
                std::make_move_iterator(dynamics.end()), dynamics.begin(),
                [&](auto &&arg) {
                  return std::visit(
                      boss::utilities::overload(
                          [&](ComplexExpression &&listExpression)
                              -> boss::Expression {
                            auto [listHead, listUnused_, listDynamics,
                                  listSpans] =
                                std::move(listExpression).decompose();

                            std::transform(
                                std::make_move_iterator(listSpans.begin()),
                                std::make_move_iterator(listSpans.end()),
                                listSpans.begin(),
                                [&](boss::expressions::ExpressionSpanArgument
                                        &&arg)
                                    -> boss::expressions::
                                        ExpressionSpanArgument {
                                          if (std::holds_alternative<
                                                  boss::Span<std::string>>(
                                                  arg)) {
                                            Dictionary &dict =
                                                dictionaries[tempHead];
                                            return std::move(dict.encode(
                                                std::move(std::get<boss::Span<
                                                              std::string>>(
                                                    std::move(arg)))));
                                          }
                                          return std::move(arg);
                                        });
                            return boss::ComplexExpression(
                                std::move(listHead), {},
                                std::move(listDynamics), std::move(listSpans));
                          },
                          [this](Symbol &&symbol) -> boss::Expression {
                            return std::move(symbol);
                          },
                          [](auto &&arg) -> boss::Expression {
                            return std::forward<decltype(arg)>(arg);
                          }),
                      std::move(arg));
                });
            return boss::ComplexExpression(
                std::move(saveHead), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

boss::Expression Engine::encodeTable(ComplexExpression &&e) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();

  std::transform(std::make_move_iterator(dynamics.begin()),
                 std::make_move_iterator(dynamics.end()), dynamics.begin(),
                 [this](auto &&arg) {
                   return encodeColumn(std::forward<decltype(arg)>(arg));
                 });
  return boss::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                 std::move(spans));
}

boss::Expression Engine::decodeColumn(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    auto tempHead = std::move(head);
	    auto saveHead = tempHead;
	    
            if (dictionaries.find(tempHead) != dictionaries.end()) {
	      Dictionary &dict = dictionaries[tempHead];

              std::transform(
                  std::make_move_iterator(dynamics.begin()),
                  std::make_move_iterator(dynamics.end()), dynamics.begin(),
                  [&](auto &&arg) {
                    return std::visit(
                        boss::utilities::overload(
                            [&](ComplexExpression &&listExpression)
                                -> boss::Expression {
                              auto [listHead, listUnused_, listDynamics,
                                    listSpans] =
                                  std::move(listExpression).decompose();

                              std::transform(
                                  std::make_move_iterator(listSpans.begin()),
                                  std::make_move_iterator(listSpans.end()),
                                  listSpans.begin(),
                                  [&](boss::expressions::ExpressionSpanArgument
                                          &&arg)
                                      -> boss::expressions::
                                          ExpressionSpanArgument {
                                            if (std::holds_alternative<
                                                    boss::Span<int32_t const>>(
                                                    arg)) {
                                              return std::move(dict.decode(
                                                  std::move(std::get<boss::Span<
                                                                int32_t const>>(
                                                      std::move(arg)))));
                                            } else if (std::holds_alternative<
                                                    boss::Span<int32_t>>(
                                                    arg)) {
                                              return std::move(dict.decode(
                                                  std::move(std::get<boss::Span<
                                                                int32_t>>(
                                                      std::move(arg)))));
                                            }
                                            return std::move(arg);
                                          });
                              return boss::ComplexExpression(
                                  std::move(listHead), {},
                                  std::move(listDynamics),
                                  std::move(listSpans));
                            },
                            [this](Symbol &&symbol) -> boss::Expression {
                              return std::move(symbol);
                            },
                            [](auto &&arg) -> boss::Expression {
                              return std::forward<decltype(arg)>(arg);
                            }),
                        std::move(arg));
                  });
            }
            return boss::ComplexExpression(
                std::move(saveHead), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

boss::Expression Engine::decodeTable(ComplexExpression &&e) {
  auto [head, unused_, dynamics, spans] = std::move(e).decompose();

  std::transform(std::make_move_iterator(dynamics.begin()),
                 std::make_move_iterator(dynamics.end()), dynamics.begin(),
                 [this](auto &&arg) {
                   return decodeColumn(std::forward<decltype(arg)>(arg));
                 });
  return boss::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                 std::move(spans));
}

boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

            if (head == "EncodeTable"_) {
              if (std::holds_alternative<boss::ComplexExpression>(
                      dynamics[0])) {
                return encodeTable(std::move(
                    std::get<boss::ComplexExpression>(std::move(dynamics[0]))));
              }
            } else if (head == "DecodeTable"_) {
              if (std::holds_alternative<boss::ComplexExpression>(
                      dynamics[0])) {
                return decodeTable(std::move(
                    std::get<boss::ComplexExpression>(std::move(dynamics[0]))));
              }
            } else if (head == "GetEncodingFor"_) {
              if (std::holds_alternative<std::string>(
                      dynamics[0]) &&
		  std::holds_alternative<boss::Symbol>(
                      dynamics[1])) {
		auto key = std::get<std::string>(std::move(dynamics[0]));
		auto colSymbol = std::get<boss::Symbol>(std::move(dynamics[1]));
		auto dictsIt = dictionaries.find(colSymbol);
		if (dictsIt == dictionaries.end()) {
		  return "InvalidKeyForColumn"_("Key"_(std::move(key)), "Column"_(std::move(colSymbol)));
		}
		auto colDict = dictionaries[colSymbol];
                return colDict.getEncoding(key);
              }
            } else if (head == "SaveTable"_) {
	      auto table = std::get<boss::ComplexExpression>(std::move(dynamics[0]));
	      auto tableSymbol = std::get<boss::Symbol>(std::move(dynamics[1]));
	      tables[tableSymbol] = std::move(table);
	      return shallowCopy(std::get<boss::ComplexExpression>(tables[tableSymbol]));
	    } else if (head == "GetTable"_) {
	      auto tableSymbol = std::get<boss::Symbol>(std::move(dynamics[0]));
	      if (tables.find(tableSymbol) == tables.end()) {
		throw std::runtime_error("No saved table called: " + tableSymbol.getName());
	      }
	      return shallowCopy(std::get<boss::ComplexExpression>(tables[tableSymbol]));
	    } else if (head == "ClearTables"_) {
	      tables.clear();
	      return "Success"_;
	    } else if (head == "GetEngineCapabilities"_) {
	      return "List"_("EncodeTable"_, "DecodeTable"_, "SaveTable"_, "GetTable"_, "ClearTables"_, "GetEncodingFor"_);
	    }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return boss::ComplexExpression(
                std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

} // namespace boss::engines::DictionaryEncoder

static auto &enginePtr(bool initialise = true) {
  static std::mutex m;
  std::lock_guard const lock(m);
  static auto engine =
      std::unique_ptr<boss::engines::DictionaryEncoder::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::DictionaryEncoder::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
