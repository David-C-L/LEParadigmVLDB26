#include "BOSSLazyLoadingCoordinatorEngine.hpp"
#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <functional>
extern "C" {
#include <BOSS.h>
}

// #define DEBUG

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::LazyLoadingCoordinator {
using EvalFunction = BOSSExpression *(*)(BOSSExpression *);
using EngineCapabilities = std::vector<Symbol>;
using std::move;
namespace utilities {} // namespace utilities

  struct RemoteTableInfo {
    std::string url;
    bool hasRangeIndex = false;
    bool hasIndices = false;
    boss::Symbol rangeStart = "None"_;
    boss::Symbol rangeEnd = "None"_;
    boss::Expression indices = "None"_;
  };
  
boss::Expression applyEngine(Expression &&e, EvalFunction eval) {
  auto *r = new BOSSExpression{std::move(e)};
  auto *oldWrapper = r;
  r = eval(r);
  delete oldWrapper;
  auto result = std::move(r->delegate);
  delete r;
  return std::move(result);
}
  
std::string RBL_PATH = "INPUT";

bool Engine::hasDependents(boss::ComplexExpression const &e) {
  bool res = false;
  std::for_each(e.getDynamicArguments().begin(), e.getDynamicArguments().end(),
                [this, &res](auto const &arg) {
                  if (std::holds_alternative<boss::ComplexExpression>(arg)) {
                    auto &child = get<boss::ComplexExpression>(arg);
                    auto it_capToEngine = capToEngineMap.find(child.getHead());
                    res |= it_capToEngine != capToEngineMap.end() ||
                           hasDependents(child);
                  }
                });
  return res;
}

boss::Expression Engine::depthFirstEvaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto isLeaf = !hasDependents(expression);
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            auto it_capToEngine = capToEngineMap.find(head);
            if (it_capToEngine == capToEngineMap.end()) {
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            }

            auto &evalFunc = evalFuncs[it_capToEngine->second];
#ifdef DEBUG
            std::cout << "ENGINE: " << it_capToEngine->second << std::endl;
            std::cout << "HEAD: " << it_capToEngine->first << std::endl;
            std::cout << "IS LEAF: " << isLeaf << std::endl;
#endif

            if (isLeaf) {
#ifdef DEBUG
              std::cout << "APPLY LEAF: " << it_capToEngine->first << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            } else {
              std::transform(std::make_move_iterator(dynamics.begin()),
                             std::make_move_iterator(dynamics.end()),
                             dynamics.begin(), [this](auto &&arg) {
                               return depthFirstEvaluate(
                                   std::forward<decltype(arg)>(arg));
                             });
#ifdef DEBUG
              std::cout << "APPLY NON-LEAF: " << it_capToEngine->first
                        << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            }
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

boss::Expression Engine::depthFirstEvaluate(Expression &&e, bool isNesting) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto isLeaf = !hasDependents(expression);
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            auto it_capToEngine = capToEngineMap.find(head);
            if (it_capToEngine == capToEngineMap.end()) {
	      
	      std::transform(std::make_move_iterator(dynamics.begin()),
			     std::make_move_iterator(dynamics.end()),
			     dynamics.begin(), [this](auto &&arg) {
			       return depthFirstEvaluate(
							 std::forward<decltype(arg)>(arg), true);
			     });
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            }

            auto &evalFunc = evalFuncs[it_capToEngine->second];
#ifdef DEBUG
	    
            std::cout << "\nENGINE: " << it_capToEngine->second << std::endl;
            std::cout << "HEAD: " << it_capToEngine->first << std::endl;
            std::cout << "IS LEAF: " << isLeaf << std::endl;
#endif
            if ((it_capToEngine->first != "Project"_ &&
                 it_capToEngine->first != "Select"_ &&
                 it_capToEngine->first != "Group"_ &&
                 it_capToEngine->first != "Join"_&&
                 it_capToEngine->first != "Top"_ &&
		 it_capToEngine->first != "Order"_) &&
                isLeaf) {
#ifdef DEBUG
              std::cout << "APPLY LEAF: " << it_capToEngine->first << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            } else if (it_capToEngine->first != "Project"_ &&
                       it_capToEngine->first != "Select"_ &&
                       it_capToEngine->first != "Group"_ &&
                       it_capToEngine->first != "Join"_ &&
                       it_capToEngine->first != "Top"_ &&
                       it_capToEngine->first != "Order"_) {
              std::transform(std::make_move_iterator(dynamics.begin()),
                             std::make_move_iterator(dynamics.end()),
                             dynamics.begin(), [this](auto &&arg) {
                               return depthFirstEvaluate(
                                   std::forward<decltype(arg)>(arg), false);
                             });
#ifdef DEBUG
              std::cout << "APPLY NON-LEAF: " << it_capToEngine->first
                        << std::endl;
#endif
              return applyEngine(std::move(boss::ComplexExpression(
                                     std::move(head), {}, std::move(dynamics),
                                     std::move(spans))),
                                 evalFunc);
            }
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return depthFirstEvaluate(
                                 std::forward<decltype(arg)>(arg), true);
                           });

            if (isNesting) {
#ifdef DEBUG
              std::cout << "NESTED EXPRESSION: " << it_capToEngine->first
                        << std::endl;
#endif	    
              return boss::ComplexExpression(
                  std::move(head), {}, std::move(dynamics), std::move(spans));
            }
#ifdef DEBUG
            std::cout << "APPLY: " << it_capToEngine->first << std::endl;
#endif
            if (head == "Group"_) {
              applyEngine("Set"_("maxThreads"_, 2), evalFunc);
            } else {
              applyEngine("Set"_("maxThreads"_, 1), evalFunc);
            }
            return applyEngine(std::move(boss::ComplexExpression(
                                   std::move(head), {}, std::move(dynamics),
                                   std::move(spans))),
                               evalFunc);
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

  boss::Expression Engine::fullyWrapInConditionals(boss::Expression &&e) {
    return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return fullyWrapInConditionals(std::forward<decltype(arg)>(arg));
                           });
	    boss::ExpressionArguments evalIfDyns;
	    evalIfDyns.push_back("HOLD"_);
	    evalIfDyns.push_back(std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans))));
	    
	    return boss::ComplexExpression("EvaluateIf"_, {}, std::move(evalIfDyns), {});
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  boss::ComplexExpression getListFromSet(std::unordered_set<boss::Symbol> &colNames) {
    boss::ExpressionArguments args;
    args.reserve(colNames.size());
    for (auto &colName : colNames) {
      args.push_back(colName);
    }

    return boss::ComplexExpression("List"_, {}, std::move(args), {});
  }
  
  boss::Expression getColumnNames(boss::Expression const &e, std::unordered_set<boss::Symbol> &colNames) {
    return std::visit(
      boss::utilities::overload(
	  [&colNames](ComplexExpression const &expression) -> boss::Expression {
            auto &dynamics = expression.getDynamicArguments();
	    if (expression.getHead().getName() == "As") {
	      for (int i = 1; i < dynamics.size(); i += 2) {
		getColumnNames(dynamics[i], colNames);
	      }
	    } else {
	      for (int i = 0; i < dynamics.size(); i++) {
		getColumnNames(dynamics[i], colNames);
	      }
	    }
	    return "Success"_;
          },
	  [&colNames](Symbol const &symbol) -> boss::Expression {
	    colNames.insert(symbol);
            return "Success"_;
          },
          [](auto const &arg) -> boss::Expression {
            return "Success"_;
          }),
      e);
  }

  RemoteTableInfo extractRemoteTableInfo(boss::ComplexExpression &&e) {
    RemoteTableInfo res;
    auto [head, unused_, dynamics, spans] =
      std::move(e).decompose();

    if (head == "RemoteTable"_ ||
	head == "RangedRemoteTable"_ ||
	head == "RemoteTableWithIndices"_ ||
	head == "RangedRemoteTableWithIndices"_) {
      res.url = std::get<std::string>(dynamics[0]);

      if (head == "RangeRemoteTable"_ || head == "RangedRemoteTableWithIndices"_) {
	res.rangeStart = std::get<boss::Symbol>(dynamics[1]);
	res.rangeEnd = std::get<boss::Symbol>(dynamics[2]);
	res.hasRangeIndex = true;
      }

      if (head == "RemoteTableWithIndices"_ || head == "RangedRemoteTableWithIndices"_) {
	res.indices = std::move(dynamics[dynamics.size() - 1]);
	res.hasIndices = true;
      }
    }
    
    return res;
  }

  boss::Expression createGatherExpression(RemoteTableInfo &remoteTabInfo, boss::ComplexExpression &&colNames) {
    boss::Symbol gatherHead = "Gather"_;
    boss::ExpressionArguments gatherArgs;
    gatherArgs.push_back(remoteTabInfo.url);
    gatherArgs.push_back(RBL_PATH);

    if (remoteTabInfo.hasRangeIndex && remoteTabInfo.hasIndices) {
      gatherHead = "GatherRanges"_;
      gatherArgs.push_back(remoteTabInfo.rangeStart);
      gatherArgs.push_back(remoteTabInfo.rangeEnd);
    } else if (remoteTabInfo.hasRangeIndex && !remoteTabInfo.hasIndices) {
      auto [listHead, unused_, listDynamics, listSpans] =
                std::move(colNames).decompose();
      listDynamics.push_back(remoteTabInfo.rangeStart);
      listDynamics.push_back(remoteTabInfo.rangeEnd);
      colNames = boss::ComplexExpression(std::move(listHead), {}, std::move(listDynamics), std::move(listSpans));
    }
    
    if (remoteTabInfo.hasIndices) {
      gatherArgs.push_back(std::move(remoteTabInfo.indices));
      remoteTabInfo.hasIndices = false;
      remoteTabInfo.indices = "None"_;
    } else {
      gatherArgs.push_back("List"_("List"_()));
    }
    
    gatherArgs.push_back(std::move(colNames));

    return boss::ComplexExpression(std::move(gatherHead), {}, std::move(gatherArgs), {});
  }

  boss::Expression createRemoteTableWithIndicesExpression(RemoteTableInfo &remoteTabInfo, boss::Expression &&dataTable) {
    boss::Symbol remoteTabHead = "RemoteTableWithIndices"_;
    boss::ExpressionArguments remoteTabArgs;
    remoteTabArgs.push_back(remoteTabInfo.url);
    remoteTabArgs.push_back(std::move(dataTable));
    if (remoteTabInfo.hasRangeIndex) {
      remoteTabHead = "RangedRemoteTableWithIndices"_;
      remoteTabArgs.push_back(remoteTabInfo.rangeStart);
      remoteTabArgs.push_back(remoteTabInfo.rangeEnd);
    }

    return boss::ComplexExpression(std::move(remoteTabHead), {}, std::move(remoteTabArgs), {});
  }

  boss::Expression Engine::translateRelationalToLazyRelational(boss::Expression &&e) {
    return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
	    std::unordered_set<boss::Symbol> colNames;
	    auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Select"_ || head == "Project"_ || head == "Top"_ || head == "Order"_) {
	      RemoteTableInfo remoteTab;
	      if (std::holds_alternative<boss::ComplexExpression>(dynamics[0])) {
		remoteTab = extractRemoteTableInfo(std::get<boss::ComplexExpression>(std::move(dynamics[0])));
	      }
	      getColumnNames(dynamics[1], colNames);
	      auto colNamesList = getListFromSet(colNames);

	      auto gatherExpr = createGatherExpression(remoteTab, std::move(colNamesList));
	      auto remoteTabIndicesExpr = createRemoteTableWithIndicesExpression(remoteTab, std::move(gatherExpr));
		
	      dynamics[0] = std::move(remoteTabIndicesExpr);
	    } else if (head == "Group"_) {
	      RemoteTableInfo remoteTab;
	      if (std::holds_alternative<boss::ComplexExpression>(dynamics[0])) {
		remoteTab = extractRemoteTableInfo(std::get<boss::ComplexExpression>(std::move(dynamics[0])));
	      }
	      for (int i = 1; i < dynamics.size(); i += 2) {
		getColumnNames(dynamics[i], colNames);
	      }
	      auto colNamesList = getListFromSet(colNames);

	      auto gatherExpr = createGatherExpression(remoteTab, std::move(colNamesList));
	      auto remoteTabIndicesExpr = createRemoteTableWithIndicesExpression(remoteTab, std::move(gatherExpr));
		
	      dynamics[0] = std::move(remoteTabIndicesExpr);
	    } else if (head == "Join"_) {
	      RemoteTableInfo remoteTab1;
	      RemoteTableInfo remoteTab2;
	      if (std::holds_alternative<boss::ComplexExpression>(dynamics[0]) &&
		  std::holds_alternative<boss::ComplexExpression>(dynamics[1])) {
		remoteTab1 = extractRemoteTableInfo(std::get<boss::ComplexExpression>(std::move(dynamics[0])));
		remoteTab2 = extractRemoteTableInfo(std::get<boss::ComplexExpression>(std::move(dynamics[1])));
	      }
	      getColumnNames(dynamics[2], colNames);
	      auto colNamesList1 = getListFromSet(colNames);
	      auto colNamesList2 = getListFromSet(colNames);

	      auto gatherExpr1 = createGatherExpression(remoteTab1, std::move(colNamesList1));
	      auto gatherExpr2 = createGatherExpression(remoteTab2, std::move(colNamesList2));

	      auto remoteTabIndicesExpr1 = createRemoteTableWithIndicesExpression(remoteTab1, std::move(gatherExpr1));
	      auto remoteTabIndicesExpr2 = createRemoteTableWithIndicesExpression(remoteTab2, std::move(gatherExpr2));
		
	      dynamics[0] = std::move(remoteTabIndicesExpr1);
	      dynamics[1] = std::move(remoteTabIndicesExpr2);
	    }
	    
	    return boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  boss::expressions::ExpressionSpanArguments Engine::partitionSpan(boss::expressions::ExpressionSpanArgument &&span, size_t maxPartitions) {
    boss::expressions::ExpressionSpanArguments res;
    std::visit([&](auto&& typedSpan) {
      size_t numRows = typedSpan.size();
      size_t rowsPerPartition = numRows / maxPartitions;
      size_t remainder = numRows % maxPartitions;
 
      size_t offset = 0;
      for (size_t i = 0; i < maxPartitions; i++) {
	size_t partSize = rowsPerPartition + (i < remainder ? 1 : 0);
	auto part = std::move(typedSpan).subspan(offset, partSize);
	res.push_back(std::move(part));
	offset += partSize;
      }
    }, std::forward<decltype(span)>(span));
    return std::move(res);
  }

  std::vector<boss::Expression> Engine::partitionTable(ComplexExpression &&e, size_t maxPartitions) {
    auto [head, unused_, dynamics, spans] =
      std::move(e).decompose();
    if (head != "Table"_) {
      throw std::runtime_error("Cannot call partitionTable on a non-Table_ complex expression.");
    }

    std::vector<ExpressionArguments> partitionedCols(maxPartitions);
    for (size_t colI = 0; colI < dynamics.size(); colI++) {
      auto [colHead, colUnused_, colDynamics, colSpans] =
	std::get<ComplexExpression>(std::move(dynamics[colI])).decompose();
      auto [listHead, listUnused_, listDynamics, listSpans] =
	std::get<ComplexExpression>(std::move(colDynamics[0])).decompose();

      boss::expressions::ExpressionSpanArguments spanPartitions;
      
      if (listSpans.size() >= maxPartitions) {
        spanPartitions = std::move(listSpans);
      } else if (listSpans.size() == 1) {
	spanPartitions = partitionSpan(std::move(listSpans[0]), maxPartitions);
      } else {
	throw std::runtime_error("When partitioning Table_, the number of spans must either be 1 or >= maxPartitions. Other counts are not yet supported. Found count: " + std::to_string(listSpans.size()));
      }

      size_t numSpansPerThread = spanPartitions.size() / maxPartitions;
      size_t remainder = spanPartitions.size() % maxPartitions;
      
      for (size_t partI = 0; partI < maxPartitions && partI < spanPartitions.size() && partI < partitionedCols.size(); partI++) {
	size_t currNumSpans = numSpansPerThread + (partI < remainder ? 1 : 0);
	
	boss::expressions::ExpressionSpanArguments partSpanArgs; 
	for (size_t spanI = 0; spanI < currNumSpans; spanI++) {
	  partSpanArgs.push_back(std::move(spanPartitions[spanI + currNumSpans * partI]));
	}
	auto listExpr = boss::ComplexExpression("List"_, {}, {}, std::move(partSpanArgs));

	ExpressionArguments partExprArgs;
	partExprArgs.push_back(std::move(listExpr));
	auto colExpr = boss::ComplexExpression(colHead, {}, std::move(partExprArgs), {});
	partitionedCols[partI].push_back(std::move(colExpr));
      }
    }

    std::vector<boss::Expression> partitionedTableExprs;
    for (size_t partI = 0; partI < maxPartitions && partI < partitionedCols.size(); partI++) {
      auto tableExpr = boss::ComplexExpression("Table"_, {}, std::move(partitionedCols[partI]), {});
      partitionedTableExprs.push_back(std::move(tableExpr));
    }

    return std::move(partitionedTableExprs);
  }

  boss::Expression Engine::mergeTables(std::vector<ComplexExpression> &&tableExprs) {
    if (tableExprs.size() <= 0) {
      return "NoTablesToMerge"_;
    }
    if (tableExprs.size() == 1) {
      return std::move(tableExprs[0]);
    }
    auto resTable = std::move(tableExprs[0]);
    auto [resHead, unused_, resDynamics, resSpans] = std::move(resTable).decompose();

    for (auto it = std::make_move_iterator(std::next(tableExprs.begin()));
	 it != std::make_move_iterator(tableExprs.end()); ++it) {
      auto [currHead, currUnused_, currDynamics, currSpans] = std::move(*it).decompose();
      assert(currDynamics.size() == resDynamics.size());

      for (size_t colI = 0; colI < resDynamics.size(); colI++) {
	auto [rColHead, rColUnused_, rColDynamics, rColSpans] =
	  std::move(std::get<ComplexExpression>(resDynamics[colI])).decompose();
	auto [cColHead, cColUnused_, cColDynamics, cColSpans] =
	  std::move(std::get<ComplexExpression>(currDynamics[colI])).decompose();

	auto [rListHead, rListUnused_, rListDynamics, rListSpans] =
	  std::move(std::get<ComplexExpression>(rColDynamics[0])).decompose();
	auto [cListHead, cListUnused_, cListDynamics, cListSpans] =
	  std::move(std::get<ComplexExpression>(cColDynamics[0])).decompose();

	std::transform(std::make_move_iterator(cListSpans.begin()),
		       std::make_move_iterator(cListSpans.end()),
		       std::back_inserter(rListSpans),
		       [](auto &&span) { return std::move(span); });

	auto resList = boss::ComplexExpression(std::move(rListHead), {}, std::move(rListDynamics), std::move(rListSpans));
	rColDynamics[0] = std::move(resList);
	
	auto resColumn = boss::ComplexExpression(std::move(rColHead), {}, std::move(rColDynamics), std::move(rColSpans));
	resDynamics[colI] = std::move(resColumn);
      }
    }
    return boss::ComplexExpression(std::move(resHead), {}, std::move(resDynamics), std::move(resSpans));
  }

  std::tuple<boss::Expression, std::vector<boss::Symbol>, std::vector<boss::Expression>>
  Engine::findAllJoinLeaves(ComplexExpression &&expr) {
    std::vector<boss::Symbol> joinPlaceholders;
    std::vector<boss::Expression> subJoinExprs;

    std::function<void(ComplexExpression&)> helper = [&](ComplexExpression &e) -> void {
      auto [head, unused_, dynamics, spans] = std::move(e).decompose();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);
	  if (complexDyn.getHead() == "Join"_) {
	    auto [joinHead, joinUnused_, joinDynamics, joinSpans] = std::move(complexDyn).decompose();

	    bool branchHasJoin1 = expressionContainsJoin(joinDynamics[0]);
	    bool branchHasJoin2 = expressionContainsJoin(joinDynamics[1]);

	    auto& joinBranch1 = std::get<ComplexExpression>(joinDynamics[0]);
	    auto& joinBranch2 = std::get<ComplexExpression>(joinDynamics[1]);

	    if (branchHasJoin1) {
	      helper(joinBranch1);
	    } else {
	      std::string joinStr = "JoinPlaceholder" + std::to_string(joinPlaceholders.size());
	      boss::Symbol joinPlaceholder(joinStr);
	      joinPlaceholders.push_back(joinPlaceholder);
	      subJoinExprs.push_back(std::move(joinBranch1));
	      joinDynamics[0] = joinPlaceholder;
	    }
	    
	    if (branchHasJoin2) {
	      helper(joinBranch2);
	    } else {
	      std::string joinStr = "JoinPlaceholder" + std::to_string(joinPlaceholders.size());
	      boss::Symbol joinPlaceholder(joinStr);
	      joinPlaceholders.push_back(joinPlaceholder);
	      subJoinExprs.push_back(std::move(joinBranch2));
	      joinDynamics[1] = joinPlaceholder;
	    }
	    auto reconstructedJoin = ComplexExpression(std::move(joinHead), {}, std::move(joinDynamics), std::move(joinSpans));
	    dynamics[i] = std::move(reconstructedJoin);
	  } else {
	    helper(complexDyn);
	  }
	}
      }
      e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    };

    helper(expr);
    if (joinPlaceholders.size() != subJoinExprs.size()) {
      throw std::runtime_error("Inconsistent join leaves extraction.");
    }
    return {std::move(expr), std::move(joinPlaceholders), std::move(subJoinExprs)};
  }

  std::tuple<boss::Expression, std::vector<boss::Symbol>, std::vector<boss::Expression>>
  Engine::findAllTableExpressions(ComplexExpression &&expr) {
    std::vector<boss::Symbol> tablePlaceholders;
    std::vector<boss::Expression> tableExprs;

    std::function<void(ComplexExpression&)> helper = [&](ComplexExpression &e) -> void {
      auto [head, unused_, dynamics, spans] = std::move(e).decompose();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  if (std::get<ComplexExpression>(dynamics[i]).getHead() == "Table"_) {
	    std::string tableStr = "TablePlaceholder" + std::to_string(tablePlaceholders.size());
	    boss::Symbol tablePlaceholder(tableStr);
	    tablePlaceholders.push_back(tablePlaceholder);
	    
	    tableExprs.push_back(std::move(dynamics[i]));
	    dynamics[i] = tablePlaceholder;
	  } else {
	    auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);
	    helper(complexDyn);
	  }
	} 
      }
        e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    };

    helper(expr);
    if (tablePlaceholders.size() != tableExprs.size()) {
      throw std::runtime_error("Inconsistent table extraction.");
    }
    return {std::move(expr), std::move(tablePlaceholders), std::move(tableExprs)};
  }

  std::tuple<boss::Symbol, boss::Expression, boss::Expression> Engine::findTableExpression(ComplexExpression &&expr) {
    if (expr.getHead() == "Table"_) {
      return {"Done"_, "TablePlaceholder"_, std::move(expr)};
    }
    auto [head, unused_, dynamics, spans] = std::move(expr).decompose();

    for (size_t i = 0; i < dynamics.size(); i++) {
      if (!std::holds_alternative<ComplexExpression>(dynamics[i])) {
	continue;
      }
      auto [status, subExpr, possTable] = findTableExpression(std::move(std::get<ComplexExpression>(dynamics[i])));
      dynamics[i] = std::move(subExpr);

      if (status == "Done"_) {
	boss::Expression surroundingExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
        return {"Done"_, std::move(surroundingExpr), std::move(possTable)};
      }
    }

    auto currExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    return {"Continue"_, std::move(currExpr), "NotATable"_()};
  }

  std::tuple<boss::Symbol, boss::Expression, ComplexExpression> inputTablePartitionIntoExpressionHelper(boss::Expression &&expr, ComplexExpression &&tableExpr) {
    return std::visit(
      boss::utilities::overload(
	  [&](ComplexExpression &&expression) -> std::tuple<boss::Symbol, boss::Expression, ComplexExpression> {
	    auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
	    for (size_t i = 0; i < dynamics.size(); i++) {
	      if (!std::holds_alternative<ComplexExpression>(dynamics[i]) && !std::holds_alternative<boss::Symbol>(dynamics[i])) {
		continue;
	      }
	      auto [status, subExpr, possTable] = inputTablePartitionIntoExpressionHelper(std::move(dynamics[i]), std::move(tableExpr));
	      dynamics[i] = std::move(subExpr);

	      if (status == "Done"_) {
		auto surroundingExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
		return {"Done"_, std::move(surroundingExpr), std::move(possTable)};
	      }

	      tableExpr = std::move(possTable);
	    }

	    auto currExpr = boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
	    return {"Continue"_, std::move(currExpr), std::move(tableExpr)};
	  },
          [&](Symbol &&symbol) -> std::tuple<boss::Symbol, boss::Expression, ComplexExpression> {
	    if (symbol == "TablePlaceholder"_) {
	      return {"Done"_, std::move(tableExpr), "EmptyTable"_()};
	    }
	    return {"Continue"_, std::move(symbol), std::move(tableExpr)};
          },
          [&](auto &&arg) -> std::tuple<boss::Symbol, boss::Expression, ComplexExpression> {
	    return {"Continue"_, std::move(arg), std::move(tableExpr)};
          }),
      std::move(expr));
  }

  boss::Expression Engine::inputTablePartitionIntoExpression(boss::Expression &&expr, boss::Expression &&tableExpr) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> boss::Expression {
	    auto [status, resExpr, possTable] = inputTablePartitionIntoExpressionHelper(std::move(expression), std::move(std::get<ComplexExpression>(tableExpr)));
	    if (status != "Done"_) {
	      throw std::runtime_error("Unable to find TablePlaceholder_ symbol.");
	    }
	    return std::move(resExpr);
	  },
          [&](Symbol &&symbol) -> boss::Expression {
	    if (symbol == "TablePlaceholder"_) {
	      return std::move(tableExpr);
	    }
	    return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(expr));
  }

  boss::Expression Engine::inputAllPlaceholdersIntoExpression(boss::Expression &&expr, const std::vector<boss::Symbol> &tablePlaceholders, std::vector<boss::Expression> &&tableExprs) {
    if (!std::holds_alternative<ComplexExpression>(expr)) {
      return std::move(expr);
    }
    
    std::function<void(ComplexExpression&)> helper = [&](ComplexExpression &e) -> void {
      auto [head, unused_, dynamics, spans] = std::move(e).decompose();
      for (size_t i = 0; i < dynamics.size(); i++) {
	if (std::holds_alternative<boss::Symbol>(dynamics[i])) {
	  const auto it = std::find(tablePlaceholders.begin(),
				    tablePlaceholders.end(),
				    std::get<boss::Symbol>(dynamics[i]));
	  if (it != tablePlaceholders.end()) {
	    size_t index = std::distance(tablePlaceholders.begin(), it);
	    dynamics[i] = std::move(tableExprs[index]);
	  }
	} else if (std::holds_alternative<ComplexExpression>(dynamics[i])) {
	  auto& complexDyn = std::get<ComplexExpression>(dynamics[i]);
	  helper(complexDyn);
	}
      }

      e = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
    };

    auto& complexDyn = std::get<ComplexExpression>(expr);
    helper(complexDyn);
    return std::move(expr);
  }
  
  std::vector<boss::Expression> Engine::getPartitionedExpressions(ComplexExpression &&expr, size_t maxPartitions) {
    // Expression may contain multiple Tables and we do not want to clone their data
    auto [outerExpr, tablePlaceholders, tableExprs] = findAllTableExpressions(std::move(expr));
    if (tableExprs.size() == 0) {
      throw std::runtime_error("Unable to find table expression to partition.");
    }

    const size_t numTables = tableExprs.size();

    std::vector<std::vector<boss::Expression>> partitionedTables;
    for (size_t i = 0; i < tableExprs.size(); i++) {
      partitionedTables.push_back(std::move(partitionTable(std::move(std::get<ComplexExpression>(tableExprs[i])), maxPartitions)));
    }
    const size_t numTablePartitions = partitionedTables[0].size();

    std::vector<std::vector<boss::Expression>> transposedPartitionedTables(numTablePartitions);
    for (size_t i = 0; i < numTablePartitions; i++) {
      std::vector<boss::Expression> partitions(numTables);
      for (size_t j = 0; j < numTables; j++) {
	partitions[j] = std::move(partitionedTables[j][i]);
      }
      transposedPartitionedTables[i] = std::move(partitions);
    }
    partitionedTables.clear();

    std::vector<boss::Expression> partitionedExpressions;
    for (size_t i = 0; i < numTablePartitions; i++) {
      auto outerExprCloned = outerExpr.clone(expressions::CloneReason::EXPRESSION_AUGMENTATION);
      auto partitionedExpression =
	inputAllPlaceholdersIntoExpression(std::move(outerExprCloned),
					      tablePlaceholders,
					      std::move(transposedPartitionedTables[i]));
      partitionedExpressions.push_back(std::move(partitionedExpression));
    }
    transposedPartitionedTables.clear();
    return std::move(partitionedExpressions);
  }

  boss::Expression Engine::mergePartitionedExpressions(std::vector<boss::Expression> &&partitionedExprs) {
    bool setOuterExpr = false;
    boss::Expression outerExpr = "OuterExprPlaceholder"_;
    std::vector<ComplexExpression> tableExprs;
    for (size_t i = 0; i < partitionedExprs.size(); i++) {
      auto [status, currOuterExpr, currTableExpr] = findTableExpression(std::move(std::get<ComplexExpression>(partitionedExprs[i])));
      if (status != "Done"_) {
	throw std::runtime_error("Unable to find table expression to merge.");
      }

      if (!setOuterExpr) {
	outerExpr = std::move(currOuterExpr);
      }

      tableExprs.push_back(std::move(std::get<ComplexExpression>(currTableExpr)));
    }
    partitionedExprs.clear();

    auto mergedTableExpr = mergeTables(std::move(tableExprs));

    return inputTablePartitionIntoExpression(std::move(outerExpr), std::move(std::get<ComplexExpression>(mergedTableExpr)));
  }
  
  bool Engine::expressionContainsSymbols(const Expression &e, const std::unordered_set<boss::Symbol> &symbols) {
    return
      std::visit(
         boss::utilities::overload(
	    [&](const ComplexExpression &expr) -> bool {
	      if (symbols.find(expr.getHead()) != symbols.end()) {
		return true;
	      }
	      bool res = false;
	      const auto &dynArgs = expr.getDynamicArguments();
	      for (const auto &arg : dynArgs) {
		res |= expressionContainsSymbols(arg, symbols);
		if (res) {
		  return res;
		}
	      }
	      return res;
	    },
	    [&](const auto &arg) -> bool {
	      return false;
	    }),
	 e);
  }

  bool Engine::expressionContainsPipelineBreaker(const Expression &e) {
    std::unordered_set<boss::Symbol> pipelineBreakers{"Join"_, "Top"_, "Order"_, "Group"_, "SaveTable"_, "GetTable"_};
    return expressionContainsSymbols(e, pipelineBreakers);
  }
  
  bool Engine::expressionContainsGather(const Expression &e) {
    std::unordered_set<boss::Symbol> gathers{"Gather"_, "__Gather"_};
    return expressionContainsSymbols(e, gathers);
  }
  
  bool Engine::expressionContainsJoin(const Expression &e) {
    std::unordered_set<boss::Symbol> joins{"Join"_, "__Join"_};
    return expressionContainsSymbols(e, joins);
  }
  
  bool Engine::expressionContainsTable(const Expression &e) {
    std::unordered_set<boss::Symbol> tables{"Table"_};
    return expressionContainsSymbols(e, tables);
  }
  
  bool Engine::expressionContainsGetOrSaveTable(const Expression &e) {
    std::unordered_set<boss::Symbol> tables{"GetTable"_, "SaveTable"_};
    return expressionContainsSymbols(e, tables);
  }
  
  bool Engine::expressionContainsUnseenTableInGather(const Expression &e, const std::unordered_set<std::string> &seenTables) {
    return
      std::visit(
         boss::utilities::overload(
	    [&](const ComplexExpression &expr) -> bool {
	      if (expr.getHead() == "Gather"_) {
		const auto& url = std::get<std::string>(expr.getDynamicArguments()[0]);
		return seenTables.find(url) == seenTables.end();
	      }
	      bool res = false;
	      const auto &dynArgs = expr.getDynamicArguments();
	      for (const auto &arg : dynArgs) {
		res |= expressionContainsUnseenTableInGather(e, seenTables);
		if (res) {
		  return res;
		}
	      }
	      return res;
	    },
	    [&](const auto &arg) -> bool {
	      return false;
	    }),
	 e);
  }

  size_t countListSpans(const ComplexExpression &e) {
    if (e.getHead() != "Table"_ || e.getDynamicArguments().size() < 1) {
      return 0;
    }

    const auto& colExpr = std::get<ComplexExpression>(e.getDynamicArguments()[0]);
    const auto& listExpr = std::get<ComplexExpression>(colExpr.getDynamicArguments()[0]);
    return listExpr.getSpanArguments().size();
  }
  
  std::pair<boss::Expression, size_t> Engine::breakExprAndCountNestedTableSpans(boss::Expression &&e) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> std::pair<boss::Expression, size_t> {
	    auto [status, outerExpr, tableExpr] = findTableExpression(std::move(expression));
	    if (status != "Done"_) {
	      throw std::runtime_error("Could not find Table_ expression in call to breakExprAndCountNestedTableSpans.");
	    }
	    const ComplexExpression &tempTableExpr = std::get<ComplexExpression>(tableExpr);
	    size_t numSpans = countListSpans(tempTableExpr);
	    auto originalExpr = inputTablePartitionIntoExpression(std::move(outerExpr), std::move(tableExpr));
	    return {std::move(originalExpr), numSpans};
	  },
          [&](auto &&arg) -> std::pair<boss::Expression, size_t> {
            return {std::forward<decltype(arg)>(arg), 0};
          }),
      std::move(e));
  }

  boss::Expression Engine::evaluateCycle(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
	    
#ifdef DEBUG	    
            std::cout << "\n\nSTAGE: " << stage << std::endl;
#endif
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    curr = inputStageValue(std::move(curr));
	    curr = applyEngine(std::move(curr), condEvalFunc);
	    curr = inputStageSymbol(std::move(curr));
	    curr = applyEngine(std::move(curr), fileFormatLoaderEvalFunc);	    
	    curr = applyEngine(std::move(curr), dictEncEvalFunc);	    	    
	    curr = applyEngine(std::move(curr), veloxEvalFunc);	    
	    stage++;

	    return std::move(evaluateCycle(std::move(curr)));
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}

  
  std::mutex wisentMutex;

  std::pair<size_t, boss::Expression> Engine::evaluateCycleWithLocalStageAndPipelineBreakStop(Expression &&e, size_t localStage) {
  return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    curr = inputLocalStageValue(std::move(curr), localStage);
	    curr = applyEngine(std::move(curr), condEvalFunc);
	    curr = inputStageSymbol(std::move(curr));

	    if (expressionContainsPipelineBreaker(curr)) {
	      return {localStage, std::move(curr)};
	    }
	    {
	      curr = inputLocalSpansOutToGather(std::move(curr), 1);
	      curr = applyEngine(std::move(curr), fileFormatLoaderEvalFunc);
	    }
	    curr = applyEngine(std::move(curr), dictEncEvalFunc);
	    
	    {
	      curr = applyEngine(std::move(curr), veloxEvalFunc);
	    }
	    localStage++;

	    return evaluateCycleWithLocalStageAndPipelineBreakStop(std::move(curr), localStage);
	  },
          [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
}

  std::pair<size_t, boss::Expression> Engine::evaluateCycleParallelControllerForNumPartitions(Expression &&e, size_t localStage, size_t maxPartitions) {
    return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
#ifdef DEBUG	    
            std::cout << "\n\nPARALLEL CONTROLLER FOR LOCAL STAGE: " << localStage << std::endl;
	    std::cout << "\n\nPARALLEL CONTROLLER FOR NUM PARTITIONS: " << maxPartitions << std::endl;
#endif

            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
	    curr = inputLocalStageValue(std::move(curr), localStage);
	    curr = applyEngine(std::move(curr), condEvalFunc);
	    curr = inputStageSymbol(std::move(curr));
	    
	    bool isPipelineBreaker = expressionContainsPipelineBreaker(curr);
	    bool hasGather = expressionContainsGather(curr);
	    bool hasTable = expressionContainsTable(curr);
	    bool isFirstStage = localStage == 0;

	    auto distributeControl = [&](boss::Expression &&exprToEval, size_t stageToEval) {
	      if (isPipelineBreaker && isFirstStage) {
		setNumberOfOutputPartitionsOnLoader(maxPartitions);
	        exprToEval = inputLocalSpansOutToGather(std::move(exprToEval), maxPartitions);
		return evaluateSingleCycle(std::move(exprToEval), stageToEval);
	      } else if (isFirstStage) {
		setNumberOfOutputPartitionsOnLoader(maxPartitions);
		exprToEval = inputLocalSpansOutToGather(std::move(exprToEval), maxPartitions);
		return evaluateSingleLoadContinueMulti(std::move(exprToEval), stageToEval, maxPartitions);
	      } else if (isPipelineBreaker) {
		if (hasGather) {
		  setNumberOfOutputPartitionsOnLoader(1);
		  exprToEval = inputLocalSpansOutToGather(std::move(exprToEval), 1);
		  return evaluateMultiLoadSingleEval(std::move(exprToEval), stageToEval, maxPartitions);
		} else {
		  return evaluateSingleEval(std::move(exprToEval), stageToEval);
		}
	      } else {
		if (hasTable) {
		  setNumberOfOutputPartitionsOnLoader(1);
		  exprToEval = inputLocalSpansOutToGather(std::move(exprToEval), 1);
		  return evaluateMultiCycle(std::move(exprToEval), stageToEval, maxPartitions);
		} else {
		  setNumberOfOutputPartitionsOnLoader(maxPartitions);
		  exprToEval = inputLocalSpansOutToGather(std::move(exprToEval), maxPartitions);
		  return evaluateSingleLoadContinueMulti(std::move(exprToEval), stageToEval, maxPartitions);
		}
	      }
	    };

	    auto [resStage, resExpr] = distributeControl(std::move(curr), localStage);
	    return std::move(evaluateCycleParallelControllerForNumPartitions(std::move(resExpr), resStage, maxPartitions));
          },
          [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
  }

boss::Expression Engine::evaluateCycleParallelController(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> boss::Expression {
	    
#ifdef DEBUG	    
            std::cout << "\n\nPARALLEL CONTROLLER FOR STAGE: " << stage << std::endl;
#endif

            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
	    }

	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    bool hasJoins = expressionContainsJoin(curr);
	    bool hasGathers = expressionContainsGather(curr);

	    if (!hasJoins) {
	      auto [resStage, resExpr] = std::move(evaluateCycleParallelControllerForNumPartitions(std::move(curr), stage, numPartitions));
	      stage = resStage;
	      return std::move(resExpr);
	    } else {
	      Expression resCurr = "Empty"_();
	      auto& complexCurr = std::get<ComplexExpression>(curr);
	      auto [outerJoinsExpr, joinPlaceholders, subJoinExprs] = findAllJoinLeaves(std::move(complexCurr));


	      auto sortJoinExprsBySmallestStage = [&](std::vector<boss::Expression>& exprs, std::vector<boss::Symbol>& placeholders) {
		std::vector<std::tuple<int32_t, boss::Expression, boss::Symbol>> decorated;
		decorated.reserve(exprs.size());

		for (size_t i = 0; i < exprs.size(); i++) {
		  int32_t smallestStage = findSmallestStage(exprs[i]); 
		  decorated.emplace_back(smallestStage, std::move(exprs[i]), std::move(placeholders[i]));
        	}

		std::sort(decorated.begin(), decorated.end(),
			  [](const auto& a, const auto& b) {
			    return std::get<0>(a) < std::get<0>(b);
			  });

		for (size_t i = 0; i < decorated.size(); i++) {
		  auto [smallestStage, expr, placeholder] = std::move(decorated[i]);
		  exprs[i] = std::move(expr);
		  placeholders[i] = std::move(placeholder);
		}
	      };
	      sortJoinExprsBySmallestStage(subJoinExprs, joinPlaceholders);
	      
	      const size_t numSubJoins = subJoinExprs.size();
	      size_t originalStage = stage;
	      if (numSubJoins > numPartitions) {

		size_t spareThreads = maxThreads - numPartitions - 1;
		setNumberOfThreadsOnLoader(spareThreads < 1 ? 1 : spareThreads);
		size_t maxStage = stage;
		for (size_t subJoinI = 0; subJoinI < numSubJoins; subJoinI++) {
		  Expression currSubJoin = std::move(subJoinExprs[subJoinI]);
		  auto [currTempStage, currTempResExpr] = std::move(evaluateCycleParallelControllerForNumPartitions(std::move(currSubJoin), stage, numPartitions));
		  if (currTempStage > maxStage) {
		    maxStage = currTempStage;
		  }
		  subJoinExprs[subJoinI] = std::move(currTempResExpr);
		}
		stage = maxStage;
		resCurr = std::move(inputAllPlaceholdersIntoExpression(std::move(outerJoinsExpr), joinPlaceholders, std::move(subJoinExprs)));
	      } else {
		size_t spareThreads = maxThreads - numSubJoins - numPartitions;
		size_t maxPartitions = numPartitions / numSubJoins;
		size_t maxAddedThreads = spareThreads / numSubJoins;

		setNumberOfThreadsOnLoader(maxAddedThreads < 1 ? 1 : maxAddedThreads);
		
		std::vector<std::future<std::tuple<size_t, size_t, boss::Expression>>> futureExprs;

		auto multiJoinLeavesExecution = [&](boss::Expression &&threadLocalExpr, size_t threadLocalMaxPartitions, size_t threadLocalStage, size_t joinI) -> std::tuple<size_t, size_t, boss::Expression> {
		  auto [resStage, resExpr] = std::move(evaluateCycleParallelControllerForNumPartitions(std::move(threadLocalExpr), threadLocalStage, threadLocalMaxPartitions));
		  return {joinI, resStage, std::move(resExpr)};
		};

		// LAUNCH SUB-EXPR EXECUTION
		for (size_t i = 0; i < subJoinExprs.size(); i++) {
		  futureExprs.push_back(std::async(std::launch::async, [&, i, expr = std::move(subJoinExprs[i])]() mutable {
		    return multiJoinLeavesExecution(std::move(expr), maxPartitions, stage, i);
		  }));
		}
	        subJoinExprs.clear();

		size_t maxStage = stage;
		std::vector<boss::Expression> resExprs(futureExprs.size());
		for (auto& future : futureExprs) {
		  auto [joinI, futStage, futExpr] = future.get();
		  if (futStage > maxStage) {
		    maxStage = futStage;
		  }
		  resExprs[joinI] = std::move(futExpr);
		}

		stage = maxStage;
		resCurr = std::move(inputAllPlaceholdersIntoExpression(std::move(outerJoinsExpr), joinPlaceholders, std::move(resExprs)));
	      }
	      
	      size_t stageDiff = stage - originalStage;
	      resCurr = std::move(incrementStageReleaseBy(std::move(resCurr), stageDiff));
	      setNumberOfThreadsOnLoader(maxThreads);
	      return evaluateCycle(std::move(resCurr));
	    }
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
} 

  std::pair<size_t, boss::Expression> Engine::evaluateSingleCycle(Expression &&e, size_t localStage) {
  return std::visit(
      boss::utilities::overload(
	 [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
#ifdef DEBUG	    
            std::cout << "\n\nSINGLE CYCLE EVAL FOR STAGE: " << localStage << std::endl;
#endif
	    
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    curr = applyEngine(std::move(curr), fileFormatLoaderEvalFunc);	    
	    curr = applyEngine(std::move(curr), dictEncEvalFunc);
	    curr = applyEngine(std::move(curr), veloxEvalFunc);	    
	    localStage++;

	    return {localStage, std::move(curr)};
          },
	 [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
	   return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
	   return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
}
  
  std::pair<size_t, boss::Expression> Engine::evaluateSingleEval(Expression &&e, size_t localStage) {
  return std::visit(
      boss::utilities::overload(
	 [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
#ifdef DEBUG	    
            std::cout << "\n\nSINGLE EVAL FOR STAGE: " << localStage << std::endl;
#endif
	    
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    
	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    curr = applyEngine(std::move(curr), dictEncEvalFunc);
	    curr = applyEngine(std::move(curr), veloxEvalFunc);	    
	    localStage++;

	    return {localStage, std::move(curr)};
          },
	 [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
	   return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
	   return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
} 

  std::pair<size_t, boss::Expression> Engine::evaluateSingleLoadContinueMulti(Expression &&e, size_t localStage, size_t maxPartitions) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
#ifdef DEBUG	    
            std::cout << "\n\nSINGLE LOAD, CONTINUE MULTI CYCLE EVAL FOR STAGE: " << localStage << std::endl;
#endif
	    
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    curr = applyEngine(std::move(curr), fileFormatLoaderEvalFunc);
	    setNumberOfOutputPartitionsOnLoader(1);	        
#ifdef DEBUG	    
            std::cout << "\nAFTER FILE FORMAT LOADER: " << std::endl;// curr << std::endl;
#endif
	    curr = applyEngine(std::move(curr), dictEncEvalFunc);

	    std::mutex printMutex;

	    if (std::holds_alternative<ComplexExpression>(curr) &&
		std::get<ComplexExpression>(curr).getHead() == "Table"_) {
	      return {localStage, std::move(curr)};
	    }
	    
	    bool hasTable = expressionContainsTable(curr);
	    if (!hasTable) {
	      localStage++;
	      return {localStage, std::move(curr)};
	    }
	    
	    auto [curr1, numSpans1] = breakExprAndCountNestedTableSpans(std::move(curr));
	    curr = std::move(curr1);
	    std::vector<boss::Expression> partitionedExprs = getPartitionedExpressions(std::move(std::get<ComplexExpression>(curr)), maxPartitions);
	    
	    std::vector<std::future<std::pair<size_t, boss::Expression>>> futureExprs;
	    
	    auto multiEvalAndContinue = [&](boss::Expression &&threadLocalExpr, size_t threadLocalStage) -> std::pair<size_t, boss::Expression> {
	      Expression threadCurr = std::move(threadLocalExpr);
	      {
		threadCurr = applyEngine(std::move(threadCurr), veloxEvalFunc);
	      }
	      {
		threadLocalStage++;
		auto [resStage, resExpr] = evaluateCycleWithLocalStageAndPipelineBreakStop(std::move(threadCurr), threadLocalStage);
	        if (std::get<boss::ComplexExpression>(resExpr).getHead() != "Table"_) {
		  resExpr = applyEngine(std::move(resExpr), fileFormatLoaderEvalFunc);
		  if (!expressionContainsGetOrSaveTable(resExpr)) {
		    resExpr = applyEngine(std::move(resExpr), dictEncEvalFunc);
		  }
	        }
		return {static_cast<size_t>(resStage), std::move(resExpr)};
	      }
	    };

	    // LAUNCH SUB CYCLES
	    for (size_t i = 0; i < partitionedExprs.size(); i++) {
	      futureExprs.push_back(std::async(std::launch::async, [&, expr = std::move(partitionedExprs[i])]() mutable {
		return multiEvalAndContinue(std::move(expr), localStage);
	      }));
	    }
	    partitionedExprs.clear();

	    // COLLECT SUB CYCLE RESULTS
	    int64_t resStage = -1;
	    std::vector<boss::Expression> resExprs;
	    resExprs.reserve(futureExprs.size());
	    for (auto& future : futureExprs) {
	      auto [futStage, futExpr] = future.get();
	      if (resStage < 0) {
		resStage = futStage;
	      }
	      if (resStage != futStage) {
		throw std::runtime_error("Misalignment of stages from parallel execution in evaluateSingleLoadContinueMulti.");
	      }
	      resExprs.push_back(std::move(futExpr));
	    }

	    if (resStage < 0) {
	      throw std::runtime_error("No stage derived from parallel execution in evaluateSingleLoadContinueMulti.");
	    }

	    auto resExpr = mergePartitionedExpressions(std::move(resExprs));

	    if (std::get<ComplexExpression>(resExpr).getHead() != "Table"_) {
	      if (expressionContainsGetOrSaveTable(resExpr)) {
		resExpr = applyEngine(std::move(resExpr), dictEncEvalFunc);
	      }
	      resExpr = applyEngine(std::move(resExpr), veloxEvalFunc);
	      resStage++;
	    }
	    
	    return {static_cast<size_t>(resStage), std::move(resExpr)};
          },
          [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
}

  std::pair<size_t, boss::Expression> Engine::evaluateMultiLoadSingleEval(Expression &&e, size_t localStage, size_t maxPartitions) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
#ifdef DEBUG	    
            std::cout << "\n\nMULTI LOAD, SINGLE EVAL FOR STAGE: " << localStage << std::endl;
#endif
	    
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }

	    int32_t &condEng = engineNames["Conditional"_];
	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &condEvalFunc = evalFuncs[condEng];
	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));
	    
	    bool hasTable = expressionContainsTable(curr);
	    if (!hasTable) {
	      localStage++;
	      return {localStage, std::move(curr)};
	    }

	    auto [curr1, numSpans1] = breakExprAndCountNestedTableSpans(std::move(curr));
	    curr = std::move(curr1);
	    std::vector<boss::Expression> partitionedExprs = getPartitionedExpressions(std::move(std::get<ComplexExpression>(curr)), maxPartitions);
	    std::vector<std::future<boss::Expression>> futureExprs;
	    
	    auto multiLoad = [&](boss::Expression &&threadLocalExpr, size_t threadLocalStage) {
	      Expression threadCurr = std::move(threadLocalExpr);
	      threadCurr = applyEngine(std::move(threadCurr), fileFormatLoaderEvalFunc);
	      threadCurr = applyEngine(std::move(threadCurr), dictEncEvalFunc);
	      return std::move(threadCurr);
	    };

	    // LAUNCH SUB CYCLES
	    for (size_t i = 0; i < partitionedExprs.size(); i++) {
	      futureExprs.push_back(std::async(std::launch::async, [&, expr = std::move(partitionedExprs[i])]() mutable {
		return multiLoad(std::move(expr), localStage);
	      }));
	    }
	    partitionedExprs.clear();

	    // COLLECT SUB CYCLE RESULTS
	    std::vector<boss::Expression> resExprs;
	    resExprs.reserve(futureExprs.size());
	    for (auto& futExpr : futureExprs) {
	      resExprs.push_back(futExpr.get());
	    }

	    auto mergedExpr = mergePartitionedExpressions(std::move(resExprs));
	    auto resExpr = applyEngine(std::move(mergedExpr), veloxEvalFunc);

	    localStage++;
	    
	    return {localStage, std::move(resExpr)};
          },
          [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
}

  std::pair<size_t, boss::Expression> Engine::evaluateMultiCycle(Expression &&e, size_t localStage, size_t maxPartitions) {
  return std::visit(
      boss::utilities::overload(
          [&, this](ComplexExpression &&expression) -> std::pair<size_t, boss::Expression> {
	    
#ifdef DEBUG	    
            std::cout << "\n\nMULTI CYCLE FOR STAGE: " << localStage << std::endl;
#endif

	    int32_t &veloxEng = engineNames["Velox"_];
	    int32_t &dictEncEng = engineNames["DictionaryEncoder"_];
	    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];

	    EvalFunction &veloxEvalFunc = evalFuncs[veloxEng];
	    EvalFunction &dictEncEvalFunc = evalFuncs[dictEncEng];
	    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];

	    auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Table"_) {
	      return {localStage, std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)))};
	    }
	    
	    Expression curr = std::move(boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans)));

	    bool hasTable = expressionContainsTable(curr);
	    if (!hasTable) {
	      localStage++;
	      return {localStage, std::move(curr)};
	    }
	    auto [curr1, numSpans1] = breakExprAndCountNestedTableSpans(std::move(curr));
	    curr = std::move(curr1);
	    std::vector<boss::Expression> partitionedExprs = getPartitionedExpressions(std::move(std::get<ComplexExpression>(curr)), maxPartitions);
	    std::vector<std::future<std::pair<size_t, boss::Expression>>> futureExprs;
	    
	    auto multiCycle = [&](boss::Expression &&threadLocalExpr, size_t threadLocalStage) -> std::pair<size_t, boss::Expression> {
	      auto [resStage, resExpr] = evaluateCycleWithLocalStageAndPipelineBreakStop(std::move(threadLocalExpr), threadLocalStage);
	      if (std::get<boss::ComplexExpression>(resExpr).getHead() != "Table"_) {
		resExpr = applyEngine(std::move(resExpr), fileFormatLoaderEvalFunc);
		if (!expressionContainsGetOrSaveTable(resExpr)) {
		  resExpr = applyEngine(std::move(resExpr), dictEncEvalFunc);
		}
	      }
	      return {static_cast<size_t>(resStage), std::move(resExpr)};
	    };

	    // LAUNCH SUB CYCLES
	    for (size_t i = 0; i < partitionedExprs.size(); i++) {
	      futureExprs.push_back(std::async(std::launch::async, [&, expr = std::move(partitionedExprs[i])]() mutable {
		return multiCycle(std::move(expr), localStage);
	      }));
	    }
	    partitionedExprs.clear();

	    // COLLECT SUB CYCLE RESULTS
	    std::vector<boss::Expression> resExprs;
	    int64_t resStage = -1;
	    resExprs.reserve(futureExprs.size());
	    for (auto& future : futureExprs) {
	      auto [futStage, futExpr] = future.get();
	      if (resStage < 0) {
		resStage = futStage;
	      }
	      if (resStage != futStage) {
		throw std::runtime_error("Misalignment of stages from parallel execution in evaluateMultiCycle.");
	      }
	      resExprs.push_back(std::move(futExpr));
	    }

	    if (resStage < 0) {
	      throw std::runtime_error("No stage derived from parallel execution in evaluateMultiCycle.");
	    }
	    auto resExpr = mergePartitionedExpressions(std::move(resExprs));
	    if (std::get<boss::ComplexExpression>(resExpr).getHead() != "Table"_) {
	      if (expressionContainsGetOrSaveTable(resExpr)) {
		resExpr = applyEngine(std::move(resExpr), dictEncEvalFunc);
	      }
	      resExpr = applyEngine(std::move(resExpr), veloxEvalFunc);
	      resStage++;
	    }

	    return {static_cast<size_t>(resStage), std::move(resExpr)};
	  },
          [&, this](Symbol &&symbol) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::move(symbol)};
          },
          [&](auto &&arg) -> std::pair<size_t, boss::Expression> {
            return {localStage, std::forward<decltype(arg)>(arg)};
          }),
      std::move(e));
}

  void Engine::setNumberOfOutputPartitionsOnLoader(int64_t numOutputPartitions) {
    auto setExpr = "SetNumSpansOut"_(numOutputPartitions);
    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];
    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
    auto resExpr = applyEngine(std::move(setExpr), fileFormatLoaderEvalFunc);
  }
  
  void Engine::setNumberOfThreadsOnLoader(int64_t numThreads) {
    auto setExpr = "SetNumThreads"_(numThreads);
    int32_t &fileFormatLoaderEng = engineNames["FileFormatLoader"_];
    EvalFunction &fileFormatLoaderEvalFunc = evalFuncs[fileFormatLoaderEng];
    auto resExpr = applyEngine(std::move(setExpr), fileFormatLoaderEvalFunc);
  }

  boss::Expression Engine::inputStageValue(Expression &&e) {
    return inputLocalStageValue(std::move(e), stage);
  }

  boss::Expression Engine::inputLocalStageValue(Expression &&e, size_t localStage) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [&, this](auto &&arg) {
                             return inputLocalStageValue(std::forward<decltype(arg)>(arg), localStage);
                           });
            return boss::ComplexExpression(
					   std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [&, this](Symbol &&symbol) -> boss::Expression {
	    if (symbol == "STAGE"_) {
	      return static_cast<int32_t>(localStage);
	    }
            return std::move(symbol);
          },
          [&](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  boss::Expression Engine::inputLocalStageValueToGather(Expression &&e, size_t localStage) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Gather"_) {
	      dynamics.push_back("LocalStage"_(static_cast<int64_t>(localStage)));
	    } else {
	    
	      std::transform(std::make_move_iterator(dynamics.begin()),
			     std::make_move_iterator(dynamics.end()),
			     dynamics.begin(), [&, this](auto &&arg) {
			       return inputLocalStageValueToGather(std::forward<decltype(arg)>(arg), localStage);
			     });
	    }
            return boss::ComplexExpression(
					   std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [&](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  boss::Expression Engine::inputLocalSpansOutToGather(Expression &&e, size_t localSpansOut) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Gather"_) {
	      dynamics.push_back(static_cast<int64_t>(localSpansOut));
	    } else {
	    
	      std::transform(std::make_move_iterator(dynamics.begin()),
			     std::make_move_iterator(dynamics.end()),
			     dynamics.begin(), [&, this](auto &&arg) {
			       return inputLocalSpansOutToGather(std::forward<decltype(arg)>(arg), localSpansOut);
			     });
	    }
            return boss::ComplexExpression(
					   std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [&](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }

  boss::Expression Engine::inputStageSymbol(Expression &&e) {
    return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Equals"_) {
	      dynamics[0] = "STAGE"_;
	      return boss::ComplexExpression(
					     std::move(head), {}, std::move(dynamics), std::move(spans));
	    }
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto &&arg) {
                             return inputStageSymbol(std::forward<decltype(arg)>(arg));
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
  
  boss::Expression Engine::incrementStageReleaseBy(Expression &&e, size_t increment) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();

	    if (head == "Equals"_ && dynamics.size() > 0 &&
		std::get<boss::Symbol>(dynamics[0]) == "STAGE"_) {
	      auto rhs = std::get<int32_t>(dynamics[1]);
	      dynamics[1] = rhs + static_cast<int32_t>(increment);
	      return boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
	    }
	    
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [&, this](auto &&arg) {
                             return incrementStageReleaseBy(std::forward<decltype(arg)>(arg), increment);
                           });
            return boss::ComplexExpression(
					   std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [&, this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [&](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
  }
  
  int32_t Engine::findSmallestStage(const Expression &e) {
    return std::visit(
      boss::utilities::overload(
	  [&, this](const ComplexExpression &expression) -> int32_t {
	    auto& head = expression.getHead();
	    auto& dynamics = expression.getDynamicArguments();

	    if (head == "Equals"_ && dynamics.size() > 0 &&
		std::get<boss::Symbol>(dynamics[0]) == "STAGE"_) {
	      auto& rhs = std::get<int32_t>(dynamics[1]);
	      return rhs;
	    }

	    int32_t res = -1;
	    for (auto& dynArg : dynamics) {
	      int32_t curr = findSmallestStage(dynArg);
	      if (res == -1) {
		res = curr;
	      } else if (curr != -1) {
		res = res < curr ? res : curr;
	      }
	    }
	    return res;
          },
          [](const Symbol &symbol) -> int32_t {
            return -1;
          },
          [](auto &&arg) -> int32_t {
            return -1;
          }),
      e);
  }

boss::Expression Engine::evaluate(Expression &&e) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression &&expression) -> boss::Expression {
            auto [head, unused_, dynamics, spans] =
                std::move(expression).decompose();
            if (head == "EvaluateInEngines"_) {

              // SETUP
              auto &evalFuncAddrs = std::get<boss::Span<int64_t>>(spans.at(0));
              for (auto [it, i] = std::tuple{evalFuncAddrs.begin(), 0};
                   it != evalFuncAddrs.end(); ++it, ++i) {
#ifdef DEBUG
                std::cout << "ENGINE: " << i << std::endl;
#endif
                auto evalFunc = reinterpret_cast<EvalFunction>(*it);
                EngineCapabilities engineCaps;
                evalFuncs[i] = evalFunc;

                auto engineCap =
                    applyEngine(boss::ComplexExpression(
                                    "GetEngineCapabilities"_, {}, {}, {}),
                                evalFunc);
                auto args = get<ComplexExpression>(engineCap).getArguments();
                std::for_each(std::make_move_iterator(args.begin()),
                              std::make_move_iterator(args.end()),
                              [this, &engineCaps](auto &&argument) {
                                engineCaps.push_back(get<Symbol>(argument));
                              });
                for (auto &cap : engineCaps) {
                  capToEngineMap[cap] = i;
#ifdef DEBUG
                  std::cout << "CAP: " << cap << std::endl;
#endif
                }
                engineCapsMap[i] = engineCaps;

		if (std::find(engineCaps.begin(), engineCaps.end(), "EvaluateIf"_) != engineCaps.end()) {
		  engineNames["Conditional"_] = i;
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "Project"_) != engineCaps.end()) {
		  engineNames["Velox"_] = i;
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "EncodeTable"_) != engineCaps.end()) {
		  engineNames["DictionaryEncoder"_] = i;
		} else if (std::find(engineCaps.begin(), engineCaps.end(), "GatherRanges"_) != engineCaps.end() || std::find(engineCaps.begin(), engineCaps.end(), "GetColumnsFromParquet"_) != engineCaps.end()) {
		  engineNames["FileFormatLoader"_] = i;
		}

              }

	      if (std::holds_alternative<boss::Symbol>(dynamics[0]) && std::get<boss::Symbol>(dynamics[0]) == "NonLECycle"_) {
		std::for_each(std::make_move_iterator(std::next(dynamics.begin())),
			      std::make_move_iterator(std::prev(dynamics.end())),
			      [this](auto &&argument) {
				depthFirstEvaluate(std::move(argument), false);
			      });

		return depthFirstEvaluate(std::move(*std::prev(dynamics.end())),
					  false);
	      }
	      
              std::for_each(std::make_move_iterator(dynamics.begin()),
                            std::make_move_iterator(std::prev(dynamics.end())),
                            [this](auto &&argument) {
			      stage = 0;
			      if (numPartitions == 1) {
				setNumberOfThreadsOnLoader(maxThreads);
				evaluateCycle(std::move(argument));
			      } else {
				evaluateCycleParallelController(std::move(argument));
			      }
                            });

	      stage = 0;
	      if (numPartitions == 1) {
		setNumberOfThreadsOnLoader(maxThreads);
		return evaluateCycle(std::move(*std::prev(dynamics.end())));
	      } else {
		return evaluateCycleParallelController(std::move(*std::prev(dynamics.end())));
	      }
	    }
            return std::move("Failure"_);
          },
          [this](Symbol &&symbol) -> boss::Expression {
            return std::move(symbol);
          },
          [](auto &&arg) -> boss::Expression {
            return std::forward<decltype(arg)>(arg);
          }),
      std::move(e));
}
} // namespace boss::engines::LazyLoadingCoordinator

static auto &enginePtr(bool initialise = true) {
  static auto engine =
      std::unique_ptr<boss::engines::LazyLoadingCoordinator::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::LazyLoadingCoordinator::Engine());
  }
  return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
