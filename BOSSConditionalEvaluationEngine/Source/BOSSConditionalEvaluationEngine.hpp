#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>

namespace boss::engines::ConditionalEvaluation {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  boss::Expression evaluate(boss::Expression &&e);

private:
  std::unordered_map<Symbol, boss::Expression> vars;

  boss::Expression evaluateArgs(boss::Expression &&e);
  boss::Expression toggleExpressionMangling(boss::Expression &&e, bool mangle);
  bool evaluateCondition(boss::Expression &&e);
  boss::Expression evaluateSetOperand(boss::Expression &&e);
  int64_t evaluateIncrementOperand(boss::Expression &&e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::ConditionalEvaluation
