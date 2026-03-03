from setuptools import setup, find_packages

setup(
    name='mkpipe-loader-dynamodb',
    version='0.1.0',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe', 'boto3'],
    include_package_data=True,
    entry_points={
        'mkpipe.loaders': [
            'dynamodb = mkpipe_loader_dynamodb:DynamoDBLoader',
        ],
    },
    description='DynamoDB loader for mkpipe.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
