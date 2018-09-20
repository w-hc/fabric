from setuptools import setup

setup(
    name='ttic_fabric',
    version='0.1.0',
    author='Haochen Wang',
    author_email='whc@uchicago.edu',
    packages=['fabric'],
    python_requires='>=3',
    install_requires=[
        'numpy',
        'torch',
        'wget',
        'tensorboardX',
    ],
    entry_points={
        'console_scripts': [
            'smit=fabric.cluster.submit:main',
            'sow=fabric.cluster.sow:main',
            'bloom=fabric.cluster.bloom:main'
        ]
    },
    zip_safe=False
)
