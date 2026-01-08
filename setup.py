import os
import glob
import numpy as np
from setuptools import setup, Extension
from Cython.Build import cythonize

# sources = glob.glob("**/*.pyx", recursive=True) # **/*.pyx 会搜索当前目录及其所有子目录下的 .pyx 文件
current_dir = os.path.abspath(os.getcwd())

extensions = [
    Extension(
        name="bt_sdk.core.client.mdapi", 
        sources=["bt_sdk/core/client/mdapi.pyx"],
        include_dirs=[np.get_include(), current_dir, "."],
        language="c++",
        extra_compile_args=["-O3", "-std=c++11"],
    ),
    Extension(
        name="bt_sdk.core.client.tdapi", 
        sources=["bt_sdk/core/client/tdapi.pyx"],
        include_dirs=[np.get_include(), current_dir, "."],
        language="c++",
        extra_compile_args=["-O3", "-std=c++11"],
    ),
     Extension(
        name="bt_sdk.core.client.async_client",  # * 表示匹配目录下所有模块
        sources=["bt_sdk/core/client/async_client.pyx"],
        include_dirs=[np.get_include(), "."],  # 包含 NumPy 和当前目录（用于查找 pxd）
        language="c++",                         # 如果使用了 vector/map，必须指定
        extra_compile_args=["-O3", "-std=c++11"]
            # "-Wno-unused-function",
            # "-Wno-unused-variable",
            # "-Wno-unused-but-set-variable",
            # "-Wno-unused-parameter",
            # "-Wno-sign-compare", # O3 极致优化，C++11 标准
    )
]


setup(
    name="bt_sdk_lib",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",       # 使用 Python 3 语法
            'boundscheck': False,        # 关闭数组越界检查（提升性能）
            'wraparound': False,         # 关闭负索引支持（提升性能）
            'initializedcheck': False,   # 关闭内存视图初始化检查
            'cdivision': True,           # 开启 C 级别除法（不检查除零，极快）
        },
        annotate=False # .html 文件，方便查看代码是否实现C 级加速
    )
)
