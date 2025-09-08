#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <mutex>
#include <future>
#include <vector>
#include <cassert>
#include <unordered_set>

namespace boss::engines::LazyLoadingCoordinator {

using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
using EngineCapabilities = std::vector<Symbol>;

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

  int32_t maxThreads = 3;
  int32_t numPartitions = 3;
  int32_t stage;
  std::unordered_map<Symbol, int32_t> engineNames;
  std::unordered_map<int32_t, EvalFunction> evalFuncs;
  std::unordered_map<int32_t, EngineCapabilities> engineCapsMap;
  std::unordered_map<Symbol, int32_t> capToEngineMap;

  // Merging & Partitioning Helpers
  std::tuple<boss::Expression, std::vector<boss::Symbol>, std::vector<boss::Expression>> findAllJoinLeaves(ComplexExpression &&expr); 
  std::tuple<boss::Expression, std::vector<boss::Symbol>, std::vector<boss::Expression>> findAllTableExpressions(ComplexExpression &&expr);
  std::tuple<boss::Symbol, boss::Expression, boss::Expression> findTableExpression(ComplexExpression &&expr);
  boss::Expression inputAllPlaceholdersIntoExpression(boss::Expression &&expr, const std::vector<boss::Symbol> &placeholders, std::vector<boss::Expression> &&exprs);
  boss::Expression inputTablePartitionIntoExpression(boss::Expression &&expr, boss::Expression &&tableExpr);
  std::pair<boss::Expression, size_t> breakExprAndCountNestedTableSpans(boss::Expression &&e);
    
  // Partitioning Funcs
  boss::expressions::ExpressionSpanArguments partitionSpan(boss::expressions::ExpressionSpanArgument &&span, size_t maxPartitions);
  std::vector<boss::Expression> partitionTable(ComplexExpression &&e, size_t maxPartitions);
  std::vector<boss::Expression> getPartitionedExpressions(ComplexExpression &&e, size_t maxPartitions);

  // Merging Funcs
  boss::Expression mergeTables(std::vector<ComplexExpression> &&tableExprs);
  boss::Expression mergePartitionedExpressions(std::vector<boss::Expression> &&partitionedExprs);

  // Parallel Cycle Funcs
  bool expressionContainsSymbols(const boss::Expression &e, const std::unordered_set<boss::Symbol> &symbols);
  bool expressionContainsPipelineBreaker(const boss::Expression &e);
  bool expressionContainsGather(const boss::Expression &e);
  bool expressionContainsJoin(const boss::Expression &e);
  bool expressionContainsTable(const boss::Expression &e);
  bool expressionContainsGetOrSaveTable(const boss::Expression &e);
  bool expressionContainsUnseenTableInGather(const boss::Expression &e, const std::unordered_set<std::string> &seenTables);
  boss::Expression evaluateCycleParallelController(Expression &&e);
  std::pair<size_t, boss::Expression> evaluateCycleParallelControllerForNumPartitions(Expression &&e, size_t localStage, size_t maxPartitions);
  std::pair<size_t, boss::Expression> evaluateCycleWithLocalStageAndPipelineBreakStop(Expression &&e, size_t localStage);
  std::pair<size_t, boss::Expression> evaluateSingleEval(Expression &&e, size_t localStage);
  std::pair<size_t, boss::Expression> evaluateSingleCycle(Expression &&e, size_t localStage);
  std::pair<size_t, boss::Expression> evaluateMultiCycle(Expression &&e, size_t localStage, size_t maxPartitions);
  std::pair<size_t, boss::Expression> evaluateSingleLoadContinueMulti(Expression &&e, size_t localStage, size_t maxPartitions);
  std::pair<size_t, boss::Expression> evaluateMultiLoadSingleEval(Expression &&e, size_t localStage, size_t maxPartitions);
  void setNumberOfOutputPartitionsOnLoader(int64_t numOutputPartitions);
  void setNumberOfThreadsOnLoader(int64_t numThreads);
  
  int32_t findSmallestStage(const Expression &e);
  boss::Expression inputStageValue(boss::Expression &&e);
  boss::Expression inputLocalStageValue(Expression &&e, size_t localStage);
  boss::Expression inputLocalStageValueToGather(Expression &&e, size_t localStage);
  boss::Expression inputLocalSpansOutToGather(Expression &&e, size_t localSpansOut);
  boss::Expression inputStageSymbol(boss::Expression &&e);
  boss::Expression incrementStageReleaseBy(Expression &&e, size_t increment);
  boss::Expression fullyWrapInConditionals(boss::Expression &&e);
  boss::Expression translateRelationalToLazyRelational(boss::Expression &&e);
  boss::Expression evaluateCycle(boss::Expression &&e);
  boss::Expression evaluateCycleParallelised(boss::Expression &&e);
  boss::Expression evaluateCycleSingleThread(boss::Expression &&e);
  boss::Expression depthFirstEvaluate(boss::Expression &&e);
  boss::Expression depthFirstEvaluate(boss::Expression &&e, bool isNesting);
  boss::Expression lazyLoadEvaluate(boss::Expression &&e);
  boss::Expression lazyLoadApplySelectionHeuristic(boss::Expression &&e);
  bool hasDependents(boss::ComplexExpression const &e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::LazyLoadingCoordinator
