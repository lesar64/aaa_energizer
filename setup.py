from setuptools import setup

setup(
    name='energizer',
    version='0.0.1dev1',
    description="Semester Project - Advanced Analytics and Applications",
    author="Student",
    author_email="student@uni-koeln.de",
    packages=["energizer"],
    install_requires=[
        'pandas',
        'click',
        'scikit-learn',
        'GDAL',
        'fiona',
        'geopandas',
        'folium',
        'pyarrow',
        'matplotlib',
        'numpy',
        'setuptools',
        'contextily',
        'scipy',
        'seaborn',
        'torch',
        'pytorch_lightning',
        'libspatialindex==1.9.3',
        'xarray',
        'h3'
    ],
    entry_points={
        #'console_scripts': ['energizer=energizer.cli:main']
    }
)