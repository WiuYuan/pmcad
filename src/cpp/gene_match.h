#pragma once
#include <algorithm>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pmcad {

class GeneMatch {
public:
    static std::unordered_map<std::string,
                              std::vector<std::string> >
    match_reference(
        const std::vector<std::string>& query,
        const std::unordered_map<
            std::string, std::vector<std::string> >& reference, bool verbose);
};

} // namespace pmcad