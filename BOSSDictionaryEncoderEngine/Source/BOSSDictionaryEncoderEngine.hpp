#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <set>
#include <unordered_map>
#include <utility>
#include <iostream>

namespace boss::engines::DictionaryEncoder {

class Engine {

public:
  Engine(Engine &) = delete;

  Engine &operator=(Engine &) = delete;

  Engine(Engine &&) = default;

  Engine &operator=(Engine &&) = delete;

  Engine() = default;

  ~Engine() = default;

  boss::Expression evaluate(boss::Expression &&e);
  boss::Expression encodeTable(ComplexExpression &&e);
  boss::Expression encodeColumn(Expression &&e);
  boss::Expression decodeTable(ComplexExpression &&e);
  boss::Expression decodeColumn(Expression &&e);

  
  struct Dictionary {
    int32_t nextId;
    std::unordered_map<std::string, int32_t> dictionary;
    std::vector<std::string> reverseDictionary;

    boss::Span<std::string const> decode(boss::Span<int32_t const> &&encoded) {
      std::vector<std::string> res;
      res.reserve(encoded.size());

      for (auto &id : encoded) {
        if (id >= 0 && id < reverseDictionary.size()) {
          res.push_back(reverseDictionary[id]);
        } else {
          res.push_back("INVALID_ENCODING");
        }
      }

      return boss::Span<std::string const>(std::move(res));
    }

    boss::Span<std::string const> decode(boss::Span<int32_t> &&encoded) {
      std::vector<std::string> res;
      res.reserve(encoded.size());

      for (auto &id : encoded) {
        if (id >= 0 && id < reverseDictionary.size()) {
          res.push_back(reverseDictionary[id]);
        } else {
          res.push_back("INVALID_ENCODING");
        }
      }

      return boss::Span<std::string const>(std::move(res));
    }

    boss::Span<int32_t> encode(boss::Span<std::string> &&input) {
      std::vector<int32_t> res;
      for (auto &str : input) {
        auto it = dictionary.find(str);
        if (it == dictionary.end()) {
          reverseDictionary.push_back(str);
          dictionary[str] = nextId;
          res.push_back(nextId);
          nextId++;
        } else {
          res.push_back(it->second);
        }
      }
      return boss::Span<int32_t>(std::move(res));
    }

    int32_t getEncoding(std::string key) {
      auto it = dictionary.find(key);
      if (it == dictionary.end()) {
	return -1;
      }
      return it->second;
    }

    Dictionary(const Dictionary &other)
        : nextId(other.nextId), dictionary(other.dictionary),
          reverseDictionary(other.reverseDictionary){};
    Dictionary &operator=(const Dictionary &other) {
      if (this != &other) {
        nextId = other.nextId;
        dictionary = other.dictionary;
        reverseDictionary = other.reverseDictionary;
      }
      return *this;
    };
    Dictionary(Dictionary &&other) noexcept
        : nextId(other.nextId), dictionary(std::move(other.dictionary)),
          reverseDictionary(std::move(other.reverseDictionary)) {
      other.nextId = 0;
    }
    Dictionary &operator=(Dictionary &&other) noexcept {
      if (this != &other) {
        nextId = other.nextId;
        dictionary = std::move(other.dictionary);
        reverseDictionary = std::move(other.reverseDictionary);
        other.nextId = 0;
      }
      return *this;
    }
    Dictionary() = default;
    ~Dictionary() = default;
  };
  
private:
  
  std::unordered_map<boss::Symbol, Dictionary> dictionaries;
  std::unordered_map<boss::Symbol, boss::Expression> tables;
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
} // namespace boss::engines::DictionaryEncoder
