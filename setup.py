from setuptools import setup, find_packages

setup(
    name='fabric',
    version='0.2.0',
    author='Haochen Wang',
    author_email='whc@uchicago.edu',
    packages=find_packages(),
    python_requires='>=3',
    install_requires=[
        # note that these requirements may not be exhaustive
        'pyyaml',
        'numpy',
        'torch',
    ],
    package_data={
        # If any package contains *.yml, include them:
        '': ['*.yml'],
    },
    entry_points={
        'console_scripts': [
            'smit=fabric.cluster.submit:main',
            'sow=fabric.deploy.sow:main',
        ]
    },
    zip_safe=False  # accessing config files without using pkg_resources.
)
