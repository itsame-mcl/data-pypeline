from setuptools import setup
from sys import argv
import pathlib


here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')

is_pdoc = any('pdoc' in arg for arg in argv)
if not is_pdoc:
    setup(
        name='data-pypeline',
        version='0.1',
        author='GENIN Alanna, LAGALLE Maxence, RAMBAUT Coline',
        author_email='alanna.genin@eleve.ensai.fr, maxence.lagalle@eleve.ensai.fr, coline.rambaut@eleve.ensai.fr',
        description='Pure Python 3 data wrangling tools with support for pipelines',
        long_description=long_description,
        long_description_content_type='text/markdown',
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: European Union Public Licence 1.2 (EUPL 1.2)',
            'Programming Language :: Python :: 3 :: Only'
            ],
        python_requires='>=3.6, <4',
        install_requires=['geojson', 'matplotlib', 'descartes'])
