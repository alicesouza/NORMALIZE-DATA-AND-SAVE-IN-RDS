from setuptools import setup

setup(
    name="glue_python_sheel_sample_module",
    version='0.1',
    install_requires=[
        'awswrangler',
        'boto3',
        'pandas',
        'python-dotenv',
        'awscli',
        'numpy==1.18.0'
    ],
    packages=['.src/utils']
)