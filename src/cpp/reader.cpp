#include "reader.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <stdexcept>

namespace fs = std::filesystem;

namespace pmcad {

std::vector<std::string> TSVReader::find_files(
    const std::string& foldername, const std::string& pattern) {
    std::vector<std::string> result;

    // 创建正则表达式
    std::regex regex_pattern(pattern);

    // 使用递归遍历目录
    for (const auto& entry :
         fs::recursive_directory_iterator(foldername)) {
        if (entry.is_regular_file()) {
            std::string filename =
                entry.path().filename().string();

            // 检查文件名是否匹配正则表达式
            if (std::regex_match(filename, regex_pattern)) {
                // 如果文件名匹配正则表达式，记录路径
                result.push_back(entry.path().string());
            }
        }
    }

    return result;
}

std::vector<std::vector<std::string> > TSVReader::read_tsv_file(
    const std::string& filename) {
    std::vector<std::vector<std::string> > data;
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " +
                                 filename);
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue; // 跳过空行

        std::stringstream ss(line);
        std::string value;
        std::vector<std::string> row;

        while (std::getline(ss, value, '\t')) {
            row.push_back(value);
        }
        data.push_back(row);
    }

    return data;
}

std::vector<std::vector<std::string> >
TSVReader::read_multi_tsv(
    const std::vector<std::string>& filelist) {
    std::vector<std::vector<std::string> > all_data;
    bool first_file = true; // 标记是否是第一个文件

    for (const auto& file : filelist) {
        try {
            auto data = read_tsv_file(file);

            if (first_file) {
                // 如果是第一个文件，保存表头并合并所有数据
                all_data = data;
                first_file =
                    false; // 设置为 false，以后不再重复添加表头
            } else {
                // 如果不是第一个文件，从第二行开始添加数据（跳过表头）
                all_data.insert(all_data.end(),
                                data.begin() + 1, data.end());
            }
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to read " << file
                      << ": " << e.what() << std::endl;
        }
    }

    return all_data; // 返回合并后的数据
}

std::vector<std::vector<std::string> > TSVReader::read_tsv_safe(
    const std::string& filename, bool skip_errors) {
    std::vector<std::vector<std::string> > data;
    std::ifstream file(filename);

    if (!file.is_open()) {
        if (skip_errors) {
            return data;
        }
        throw std::runtime_error("Cannot open file: " +
                                 filename);
    }

    std::string line;
    size_t line_num = 0;

    while (std::getline(file, line)) {
        line_num++;

        if (line.empty()) continue;

        try {
            std::stringstream ss(line);
            std::string value;
            std::vector<std::string> row;

            while (std::getline(ss, value, '\t')) {
                row.push_back(value);
            }
            data.push_back(row);
        } catch (const std::exception& e) {
            if (!skip_errors) {
                throw std::runtime_error(
                    "Error reading line " +
                    std::to_string(line_num) + " in " +
                    filename);
            }
        }
    }

    return data;
}

std::vector<std::vector<double> > TSVReader::read_tsv_as_double(
    const std::string& filename) {
    auto string_data = read_tsv_file(filename);
    std::vector<std::vector<double> > result;

    for (const auto& row : string_data) {
        std::vector<double> double_row;
        for (const auto& cell : row) {
            try {
                double_row.push_back(std::stod(cell));
            } catch (const std::exception&) {
                double_row.push_back(
                    0.0); // 转换失败时使用默认值
            }
        }
        result.push_back(double_row);
    }

    return result;
}

std::vector<std::vector<int> > TSVReader::read_tsv_as_int(
    const std::string& filename) {
    auto string_data = read_tsv_file(filename);
    std::vector<std::vector<int> > result;

    for (const auto& row : string_data) {
        std::vector<int> int_row;
        for (const auto& cell : row) {
            try {
                int_row.push_back(std::stoi(cell));
            } catch (const std::exception&) {
                int_row.push_back(0); // 转换失败时使用默认值
            }
        }
        result.push_back(int_row);
    }

    return result;
}

} // namespace pmcad