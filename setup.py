from setuptools import setup, find_packages

setup(
    name='bt_sdk',  # Replace with your package name
    version='0.1.0',  # Initial version
    author='Your Name',
    author_email='your.email@example.com',
    description='A brief description of your package',
    long_description=open('README.md').read(),  # Ensure you have a README.md file
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/bt_sdk',  # Replace with your repository URL
    packages=find_packages(),  # Automatically find packages in the directory
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',  # Choose your license
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',  # Specify the minimum Python version
    install_requires=[
        'pyzmq',  # Add other dependencies your project requires
        'asyncio',
        # Add more dependencies as needed
    ],
    extras_require={
        'dev': [
            'pytest',  # Development dependencies
            'flake8',
            # Add more development dependencies as needed
        ],
    },
    entry_points={
        'console_scripts': [
            'bt_sdk=bt_sdk.core.async_client:main',  # Example entry point
        ],
    },
)