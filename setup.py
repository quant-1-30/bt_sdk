from setuptools import setup, find_packages

setup(
    name='bt_sdk',
    version='0.1.0',
    author='Hengxin Liu',
    author_email='bt_sdk@example.com',
    description='BT SDK Package',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/bt_sdk',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    python_requires='>=3.6',
    install_requires=[
        'pyzmq',
        'asyncio',
        'devpi-server',
        'devpi-web',
        'devpi-client',
        'poetry',
        'ping3>=4.0.0',
    ],
    extras_require={
        'dev': [
            'pytest',
            'flake8',
            'black',
            'isort',
            'mypy',
        ],
    },
    entry_points={
        'console_scripts': [
            'bt_sdk=bt_sdk.core.async_client:main',
        ],
    },
    package_data={
        'bt_sdk': ['py.typed'],
    },
    zip_safe=False,
)