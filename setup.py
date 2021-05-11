from setuptools import setup
import pathlib


here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='data-pypeline',
    version='0.1',
    author='GENIN Alanna, LAGALLE Maxence, RAMBAUT Coline',
    author_email='alanna.genin@eleve.ensai.fr, maxence.lagalle@eleve.ensai.fr, coline.rambaut@eleve.ensai.fr',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3 :: Only'
        ],
    python_requires='>=3.6, <4',
    install_requires=['geojson', 'matplotlib', 'descartes'])
