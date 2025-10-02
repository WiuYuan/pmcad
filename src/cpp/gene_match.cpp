#include "gene_match.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pmcad {

class FastPrefixSuffixMatcher {
private:
    std::unordered_map<std::string, std::vector<std::string> >
        prefix_map;
    std::unordered_map<std::string, std::vector<std::string> >
        suffix_map;

public:
    void build(const std::unordered_map<
               std::string, std::vector<std::string> >&
                   normalized_ref) {
        // 为每个reference生成所有可能的前缀和后缀
        for (const auto& [ref, vals] : normalized_ref) {
            if (ref.empty()) continue;

            // 生成所有前缀
            for (size_t len = 1; len <= ref.length(); ++len) {
                std::string prefix = ref.substr(0, len);
                prefix_map[prefix].insert(
                    prefix_map[prefix].end(), vals.begin(),
                    vals.end());
            }

            // 生成所有后缀
            for (size_t start = 0; start < ref.length();
                 ++start) {
                std::string suffix = ref.substr(start);
                suffix_map[suffix].insert(
                    suffix_map[suffix].end(), vals.begin(),
                    vals.end());
            }
        }
    }

    // O(1) 前缀匹配（从长到短）
    void findPrefixMatches(const std::string& query,
                           std::vector<std::string>& results) {
        // 检查query的所有前缀，从最长到最短
        for (size_t len = query.length(); len >= 1; --len) {
            std::string prefix = query.substr(0, len);
            auto it = prefix_map.find(prefix);
            if (it != prefix_map.end()) {
                results.insert(results.end(),
                               it->second.begin(),
                               it->second.end());
                // 找到最长匹配后立即返回，避免找到更短的匹配
                return;
            }
        }
    }

    // O(1) 后缀匹配（从长到短）
    void findSuffixMatches(const std::string& query,
                           std::vector<std::string>& results) {
        // 检查query的所有后缀，从最长到最短
        for (size_t start = 0; start < query.length();
             ++start) {
            std::string suffix = query.substr(start);
            auto it = suffix_map.find(suffix);
            if (it != suffix_map.end()) {
                results.insert(results.end(),
                               it->second.begin(),
                               it->second.end());
                // 找到最长匹配后立即返回
                return;
            }
        }
    }
};

// 定义正则表达式
std::regex alpha_beta_gamma_pattern(
    "(alpha|beta|gamma)(?=\\s|$)");
std::regex dash_digit_pattern("-\\d+(?=\\s|$)");
std::regex digit_letter_pattern("(\\d+)[a-z](?=\\s|$)");
std::regex dash_alpha_pattern("-[a-z](?=\\s|$)");
std::regex whitespace_pattern("\\s+");

// 规范化参考字符串（移除 alpha, beta, gamma, 数字后缀等）
std::string normalize_reference(const std::string& ref) {
    std::string normalized_ref = ref;
    std::transform(normalized_ref.begin(), normalized_ref.end(),
                   normalized_ref.begin(), ::tolower);
    normalized_ref = std::regex_replace(
        normalized_ref, alpha_beta_gamma_pattern, "");
    normalized_ref = std::regex_replace(normalized_ref,
                                        dash_digit_pattern, "");
    normalized_ref = std::regex_replace(
        normalized_ref, digit_letter_pattern, "$1");
    normalized_ref = std::regex_replace(normalized_ref,
                                        dash_alpha_pattern, "");
    std::replace(normalized_ref.begin(), normalized_ref.end(),
                 '-', ' ');
    std::replace(normalized_ref.begin(), normalized_ref.end(),
                 '_', ' ');
    normalized_ref = std::regex_replace(
        normalized_ref, whitespace_pattern, " ");
    normalized_ref = std::regex_replace(
        normalized_ref, std::regex("^\\s+|\\s+$"), "");
    return normalized_ref;
}

// 规范化查询字符串
std::string normalize_query(const std::string& query_str) {
    std::string normalized_query = query_str;
    std::transform(normalized_query.begin(),
                   normalized_query.end(),
                   normalized_query.begin(), ::tolower);
    normalized_query = std::regex_replace(
        normalized_query, alpha_beta_gamma_pattern, "");
    normalized_query = std::regex_replace(
        normalized_query, dash_digit_pattern, "");
    normalized_query = std::regex_replace(
        normalized_query, digit_letter_pattern, "$1");
    normalized_query = std::regex_replace(
        normalized_query, dash_alpha_pattern, "");
    std::replace(normalized_query.begin(),
                 normalized_query.end(), '-', ' ');
    std::replace(normalized_query.begin(),
                 normalized_query.end(), '_', ' ');
    normalized_query = std::regex_replace(
        normalized_query, whitespace_pattern, " ");
    normalized_query = std::regex_replace(
        normalized_query, std::regex("^\\s+|\\s+$"), "");
    return normalized_query;
}

std::unordered_map<std::string, std::vector<std::string> >
GeneMatch::match_reference(
    const std::vector<std::string>& query,
    const std::unordered_map<
        std::string, std::vector<std::string> >& reference,
    bool verbose) {
    std::unordered_map<std::string, std::vector<std::string> >
        result;

    // 预处理参考数据：normalize并合并相同key的value，同时构建扩展字典
    std::unordered_map<std::string, std::vector<std::string> >
        normalized_reference;
    std::unordered_map<std::string, std::vector<std::string> >
        expanded_reference;

    int current = 0;
    int total_references = reference.size();

    for (const auto& [ref_key, ref_vals] : reference) {
        if (verbose) {
            float progress = static_cast<float>(current + 1) /
                             total_references;
            int bar_width = 50;
            int pos = bar_width * progress;

            std::cout << "\rProcessing references: [";
            for (int i = 0; i < bar_width; ++i) {
                if (i < pos)
                    std::cout << "=";
                else if (i == pos)
                    std::cout << ">";
                else
                    std::cout << " ";
            }
            std::cout << "] " << std::setw(3)
                      << int(progress * 100) << "% ("
                      << current + 1 << "/" << total_references
                      << ")";
            std::flush(std::cout);
        }

        std::string ref_norm = normalize_reference(ref_key);

        // 1. 添加到normalized_reference
        normalized_reference[ref_norm].insert(
            normalized_reference[ref_norm].end(),
            ref_vals.begin(), ref_vals.end());

        // 2. 构建扩展字典：将reference拆分为所有连续子序列
        // 分割参考key为单词序列
        std::vector<std::string> words;
        size_t start = 0, end;
        while ((end = ref_norm.find(' ', start)) !=
               std::string::npos) {
            words.push_back(
                ref_norm.substr(start, end - start));
            start = end + 1;
        }
        words.push_back(ref_norm.substr(start));

        // 生成所有可能的连续子序列
        for (size_t i = 0; i < words.size(); ++i) {
            for (size_t j = i; j < words.size(); ++j) {
                // 构建子序列
                std::string subsequence;
                for (size_t k = i; k <= j; ++k) {
                    if (k > i) subsequence += " ";
                    subsequence += words[k];
                }

                // 添加到扩展字典
                expanded_reference[subsequence].insert(
                    expanded_reference[subsequence].end(),
                    ref_vals.begin(), ref_vals.end());
            }
        }

        current++;
    }
    if (verbose) {
        std::cout << std::endl;
    }
    FastPrefixSuffixMatcher matcher;
    matcher.build(normalized_reference);

    // 处理每个查询
    size_t total_queries = query.size();
    for (size_t idx = 0; idx < query.size(); ++idx) {
        const std::string& q = query[idx];
        if (verbose) {
            float progress =
                static_cast<float>(idx + 1) / total_queries;
            int bar_width = 50; // 进度条宽度
            int pos = bar_width * progress;

            // 清除当前行并打印新的进度条
            std::cout << "\rProcessing queries: [";
            for (int i = 0; i < bar_width; ++i) {
                if (i < pos)
                    std::cout << "=";
                else if (i == pos)
                    std::cout << ">";
                else
                    std::cout << " ";
            }
            std::cout << "] " << std::setw(3)
                      << int(progress * 100) << "% (" << idx + 1
                      << "/" << total_queries << ")";
            std::flush(std::cout); // 确保输出立即更新
        }
        std::string q_norm = normalize_query(q);

        // 首先尝试直接匹配
        auto direct_match = normalized_reference.find(q_norm);
        if (direct_match != normalized_reference.end()) {
            result[q] = direct_match->second;
            continue;
        }

        // 分割查询字符串为单词序列
        std::vector<std::string> words;
        size_t start = 0, end;
        while ((end = q_norm.find(' ', start)) !=
               std::string::npos) {
            words.push_back(q_norm.substr(start, end - start));
            start = end + 1;
        }
        words.push_back(q_norm.substr(start));

        // 生成查询的所有可能连续子序列，并在reference中查找
        bool judge = false;
        for (size_t i = words.size(); i > 0; --i) {
            for (size_t j = 0; i + j <= words.size(); ++j) {
                // 构建子序列
                std::string subsequence;
                for (size_t k = j; k < i + j; ++k) {
                    if (k > j) subsequence += " ";
                    subsequence += words[k];
                }

                // 在normalized_reference中查找这个子序列
                auto ref_match =
                    normalized_reference.find(subsequence);
                if (ref_match != normalized_reference.end()) {
                    result[q].insert(result[q].end(),
                                     ref_match->second.begin(),
                                     ref_match->second.end());
                    judge = true;
                }
            }
            if (judge) {
                break;
            }
        }
        if (judge) continue;

        auto expanded_match = expanded_reference.find(q);
        if (expanded_match != expanded_reference.end()) {
            result[q].insert(result[q].end(),
                             expanded_match->second.begin(),
                             expanded_match->second.end());
            judge = true;
        }
        if (judge) continue;

        for (size_t i = words.size(); i > 0; --i) {
            for (size_t j = 0; i + j <= words.size(); ++j) {
                // 构建子序列
                std::string subsequence;
                for (size_t k = j; k < i + j; ++k) {
                    if (k > j) subsequence += " ";
                    subsequence += words[k];
                }

                // 在normalized_reference中查找这个子序列
                auto ref_match =
                    expanded_reference.find(subsequence);
                if (ref_match != expanded_reference.end()) {
                    result[q].insert(result[q].end(),
                                     ref_match->second.begin(),
                                     ref_match->second.end());
                    judge = true;
                }
            }
        }
        if (judge) continue;

        std::vector<std::string> prefix_matches, suffix_matches;

        // 高效前缀匹配
        matcher.findPrefixMatches(q_norm, prefix_matches);
        // 高效后缀匹配
        matcher.findSuffixMatches(q_norm, suffix_matches);

        if (!prefix_matches.empty() ||
            !suffix_matches.empty()) {
            result[q].insert(result[q].end(),
                             prefix_matches.begin(),
                             prefix_matches.end());
            result[q].insert(result[q].end(),
                             suffix_matches.begin(),
                             suffix_matches.end());
        }
    }

    if (verbose) {
        std::cout << std::endl;
    }

    return result;
}

} // namespace pmcad