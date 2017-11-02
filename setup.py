from distutils.core import setup

setup(
    name='ansible-dynamic-inventory-mysql',
    packages=['ansible-dynamic-inventory-mysql'],
    package_dir={'': '..'},
    version='0.1.0',
    python_requires='>=3',
    description='This is a Dynamic Inventory for Ansible to be used together with MySQL.',
    author='Askdaddy',
    author_email='askdaddy@gmail.com',
    url='https://github.com/askdaddy/ansible-dynamic-inventory-mysql',
    download_url='https://github.com/askdaddy/ansible-dynamic-inventory-mysql/archive/arya.zip',
    install_requires=[
        'configparser>=3.5.0',
        'PyMySQL>=0.7.11'
      ],
    keywords=['ansible','inventory','mysql']
)
