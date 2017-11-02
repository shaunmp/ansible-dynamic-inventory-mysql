from distutils.core import setup

setup(
    name='ansible-dynamic-inventory-mysql',
    packages=['ansible-dynamic-inventory-mysql'],
    version='0.0.1',
    description='ansible-dynamic-inventory-mysql',
    author='None',
    author_email='None',
    maintainer='ansible-dynamic-inventory-mysql',
    maintainer_email='',
    url='https://github.com/askdaddy/ansible-dynamic-inventory-mysql',
    download_url='https://github.com/askdaddy/ansible-dynamic-inventory-mysql/archive/arya.zip',
    install_requires=[
        'configparser>=3.5.0',
        'PyMySQL>=0.7.11'
      ],
)