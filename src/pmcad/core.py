from typing import List, Union, Dict
import pandas as pd
from ._core import read_multi_tsv
from ._core import find_files as _find_files
from ._core import match_reference as _match_reference
import os


def read_tsv_files(filelist: List[str]) -> pd.DataFrame:
    """Read multiple TSV files and return a single DataFrame"""
    # 调用 read_multi_tsv 读取所有文件的数据
    data = read_multi_tsv(filelist)  # 假设返回的是一个合并后的数据列表

    # 如果数据非空，使用第一行作为列名
    if data:
        columns = data[0]  # 假设第一行是列名
        return pd.DataFrame(data[1:], columns=columns)  # 返回合并后的 DataFrame
    else:
        return pd.DataFrame()  # 如果没有数据，返回一个空 DataFrame


def create_dict(
    x: List[str], y: List[str], splitx_by: Union[List[str], str] = []
) -> dict:
    """
    创建一个字典，将 x 中的每个元素映射到 y 中的相应元素，
    如果 splitx_by 中提供了分隔符，会将 x 中的元素按这些分隔符拆分，拆分后的子元素去除前后空格后也映射到相同的 y 元素。

    参数:
        x (List[str]): 用作字典键的列表
        y (List[str]): 用作字典值的列表
        splitx_by (Optional[Union[List[str], str]], optional):
            用于拆分 x 中每个元素的分隔符，可以是字符串（单一分隔符）或字符串列表（多个分隔符），默认为空列表

    返回:
        dict: 映射后的字典
    """

    # 处理 splitx_by 类型为列表或字符串
    if isinstance(splitx_by, str):  # 如果是单个分隔符
        splitx_by = [splitx_by]

    result_dict = {}

    # 遍历 x 和 y，假设 x 和 y 长度一致
    for xi, yi in zip(x, y):
        if not isinstance(xi, str):  # 检查 xi 是否为字符串
            xi = str(xi)
        if xi == "nan":
            continue
        # 如果指定了分隔符，则拆分 x 中的元素
        if splitx_by:
            for delimiter in splitx_by:
                xi_split = xi.split(delimiter)  # 按指定分隔符拆分
                for sub_x in xi_split:
                    sub_x = sub_x.strip().lower()  # 去除前后空格并转为小写
                    if sub_x in result_dict:
                        result_dict[sub_x].append(
                            yi
                        )  # 如果已经存在，添加到现有的列表中
                    else:
                        result_dict[sub_x] = [yi]  # 如果不存在，创建新的映射
        else:
            xi = xi.strip().lower()  # 如果没有分隔符，直接将元素映射，并去除前后空格
            if xi in result_dict:
                result_dict[xi].append(yi)
            else:
                result_dict[xi] = [yi]

    return result_dict


def match_reference(
    query: List[str], reference: Dict[str, List[str]], verbose: bool = False
) -> Dict[str, List[str]]:
    return _match_reference(query, reference, verbose)


def find_files(foldername: str, pattern: str) -> List[str]:
    return _find_files(foldername, pattern)
