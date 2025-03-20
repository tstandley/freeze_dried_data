from setuptools import setup

# Read the contents of your README file
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='freeze_dried_data',
    version='2.6.2',
    description='A simple format for machine learning datasets',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/tstandley/freeze_dried_data',
    author='Trevor Standley',
    author_email='trevor.standley@gmail.com',
    license='MIT',
    packages=['freeze_dried_data'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
