import setuptools

with open('README.md', 'r') as readme:
    description = readme.read()

setuptools.setup(
    name='bibliograph',
    version='0.0.0',
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
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English'
    ],
    install_requires=[
        'bibtexparser>=1.3.0',
        'numpy>=1.23.1',
        'pandas>=1.4.3',
        'pyparsing>=3.0.9',
        'python-dateutil>=2.8.2',
        'pytz>=2022.1',
        'six>=1.16.0'
    ]
)
