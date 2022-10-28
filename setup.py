import setuptools

with open('README.md', 'r') as readme:
    description = readme.read()

setuptools.setup(
    name='bibliograph',
    version='0.1.1',
    author='Devin Short',
    author_email='short.devin@gmail.com',
    packages=['bibliograph'],
    description=(
        'A database system for research in the humanities'
    ),
    long_description=description,
    long_description_content_type="text/markdown",
    url='https://github.com/shortorian/bibliograph',
    license='MIT',
    python_requires='>=3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English'
    ],
    install_requires=[
        'attrs>=22.1.0',
        'bibtexparser>=1.4.0',
        'colorama>=0.4.6',
        'exceptiongroup>=1.0.0rc9',
        'iniconfig>=1.1.1',
        'numpy>=1.23.4',
        'packaging>=21.3',
        'pandas>=1.5.1',
        'pluggy>=1.0.0',
        'pyparsing>=3.0.9',
        'pytest>=7.2.0',
        'python-dateutil>=2.8.2',
        'pytz>=2022.5',
        'six>=1.16.0',
        'tomli>=2.0.1'
    ]
)
