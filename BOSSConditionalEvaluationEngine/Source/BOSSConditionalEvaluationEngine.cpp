#include "BOSSConditionalEvaluationEngine.hpp"
#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <unordered_map>
#include <iostream>

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::ConditionalEvaluation {
using std::move;
namespace utilities {
static boss::Expression shallowCopy(boss::Expression const &expression);
static boss::ComplexExpression
shallowCopyComplex(boss::ComplexExpression const &expression);

static boss::ComplexExpression
shallowCopyComplex(boss::ComplexExpression const &expression) {
  auto const &head = expression.getHead();
  auto const &dynamics = expression.getDynamicArguments();
  auto const &spans = expression.getSpanArguments();
  boss::ExpressionArguments dynamicsCopy;
  std::transform(dynamics.begin(), dynamics.end(),
                 std::back_inserter(dynamicsCopy), shallowCopy);
  boss::expressions::ExpressionSpanArguments spansCopy;
  std::transform(
      spans.begin(), spans.end(), std::back_inserter(spansCopy),
      [](auto const &span) {
        return std::visit(
            [](auto const &typedSpan)
                -> boss::expressions::ExpressionSpanArgument {
              using SpanType = std::decay_t<decltype(typedSpan)>;
              using T = std::remove_const_t<typename SpanType::element_type>;
              if constexpr (std::is_same_v<T, bool>) {
                return SpanType(typedSpan.begin(), typedSpan.size(), []() {});
              } else {
                auto *ptr = const_cast<T *>(typedSpan.begin()); // NOLINT
                return boss::Span<T>(ptr, typedSpan.size(), []() {});
              }
            },
            span);
      });
  return boss::ComplexExpression(head, {}, std::move(dynamicsCopy),
                                 std::move(spansCopy));
}

static boss::Expression shallowCopy(boss::Expression const &expression) {
  return std::visit(
      boss::utilities::overload(
          [&](boss::ComplexExpression const &e) -> boss::Expression {
            return shallowCopyComplex(e);
          },
          [](auto const &arg) -> boss::Expression { return arg; }),
      expression);
}
} // namespace utilities

bool Engine::evaluateCondition(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return evaluate(std::forward<decltype(arg)>(arg));
                           });
            return evaluateCondition(
                std::move(evaluate(std::move(boss::ComplexExpression(
                    std::move(head), {}, std::move(dynamics),
                    std::move(spans))))));
          },
          [this](Symbol &&symbol) {
            auto it = vars.find(symbol);
            if (it != vars.end() && std::holds_alternative<bool>(it->second)) {
              return std::get<bool>(it->second);
            }
            return false;
          },
          [this](bool result) { return result; },
          [](auto &&arg) { return false; }),
      std::move(e));
}

boss::Expression Engine::evaluateSetOperand(boss::Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            return evaluate(std::move(expression));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            auto it = vars.find(symbol);
            if (it == vars.end()) {
              throw std::runtime_error(
                  "Conditional Evaluation: Attempt to assign value of unknown "
                  "symbol in Set expression");
            }
            return utilities::shallowCopy(it->second);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

int64_t Engine::evaluateIncrementOperand(boss::Expression &&e) {
  return std::visit(boss::utilities::overload(
                        [this](ComplexExpression &&expression) {
                          return evaluateIncrementOperand(
                              std::move(evaluate(std::move(expression))));
                        },
                        [this](Symbol &&symbol) {
                          auto it = vars.find(symbol);
                          if (it == vars.end()) {
                            throw std::runtime_error(
                                "Conditional Evaluation: Attempt to assign "
                                "value of unknown symbol in Set expression");
                          }
                          return evaluateIncrementOperand(
                              std::move(utilities::shallowCopy(it->second)));
                        },
                        [this](int64_t &&arg) { return arg + 1; },
                        [this](int32_t &&arg) { return ((int64_t)arg + 1); },
                        [this](int8_t &&arg) { return ((int64_t)arg + 1); },
                        [](auto &&arg) {
                          return (int64_t)0;
                        }),
                    std::move(e));
}

std::string removePrefix(std::string str, const std::string& prefix) {
    if (str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix) {
        str.erase(0, prefix.size());
    }
    return str;
}

std::string addPrefix(const std::string& str, const std::string& prefix) {
  if (str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix) {
    return str;
  }
  return prefix + str;
}
  
boss::Expression Engine::toggleExpressionMangling(Expression &&e, bool mangle) {
  return std::visit(
      boss::utilities::overload(
				[this, &mangle](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (mangle) {
	      head = boss::Symbol(addPrefix(head.getName(), "__"));
	    } else {
	      head = boss::Symbol(removePrefix(head.getName(), "__"));
	    }
	    
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

boss::Expression Engine::evaluateArgs(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
           
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
  
boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "Set"_) {
              auto &varSymbol = std::get<boss::Symbol>(dynamics[0]);
              vars[varSymbol] = evaluateSetOperand(std::move(dynamics[1]));
              return utilities::shallowCopy(vars[varSymbol]);
            } else if (head == "Increment"_) {
              return evaluateIncrementOperand(std::move(dynamics[0]));
            } else if (head == "Equals"_) {
              auto first = evaluate(std::move(dynamics[0]));
              auto second = evaluate(std::move(dynamics[1]));
              return first == second;
            } else if (head == "NotEquals"_) {
              auto first = evaluate(std::move(dynamics[0]));
              auto second = evaluate(std::move(dynamics[1]));
              return first != second;
            } else if (head == "EvaluateIf"_) {
              if (dynamics.empty()) {
                throw std::runtime_error(
                    "Conditional Evaluation: No "
                    "subexpressions in EvaluateIf_ expression");
              }
              auto condResult = evaluateCondition(
                  std::move(utilities::shallowCopy(dynamics[0])));
              if (condResult) {
		dynamics[1] = std::move(toggleExpressionMangling(std::move(dynamics[1]), false));
                return evaluate(std::move(dynamics[1]));
              }
	      
	      dynamics[1] = std::move(toggleExpressionMangling(std::move(dynamics[1]), true));
	      dynamics[1] = std::move(evaluateArgs(std::move(dynamics[1])));
	      
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            } else if (head == "GetEngineCapabilities"_) {
	      return "List"_("Set"_, "Increment"_, "Equals"_, "NotEquals"_, "EvaluateIf"_);
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
            auto it = vars.find(symbol);
            if (it == vars.end()) {
              return std::move(symbol);
            }
            return utilities::shallowCopy(it->second);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}
} // namespace boss::engines::ConditionalEvaluation

static auto &enginePtr(bool initialise = true) {
  static std::mutex m;
  std::lock_guard const lock(m);
  static auto engine =
      std::unique_ptr<boss::engines::ConditionalEvaluation::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::ConditionalEvaluation::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
