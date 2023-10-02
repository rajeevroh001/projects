## Getting Started

### Prerequisites

- Python 3
- pip


### To set up a virtual environment and activate it, follow these steps:

```bash
# Install virtualenv
python3 -m pip install virtualenv

# Create a virtual environment
virtualenv env

# Activate the virtual environment (Mac)
source env/bin/activate

# Activate the virtual environment (Windows)
env\Scripts\activate
```

## Folder Structure

* #### Input data
    * [inputs/dataset1.csv](inputs/dataset1.csv)
    * [inputs/dataset2.csv](inputs/dataset2.csv)
* #### Output (Note: Once output files are generated, access them here.)
    * [outputs/apache_Beam_output.csv](./outputs/apache_beam_output.csv)
    * [outputs/pandas_output.csv](./outputs/pandas_output.csv)
* [src/apache_beam_sol.py](src/apache_beam_sol.py)
* [src/pandas_sol.py](src/pandas_sol.py)
* [README.md](./README.md)
* [requirements.txt](./requirements.txt)
* [sample_test.txt](./sample_test.txt)


## Installing Required Packages
To install the required packages from the requirements.txt file, run the following command:
```bash
pip install -r requirements.txt
```

## Commands 
To Execute Files

```bash 
python src/apache_beam_sol.py;
python src/pandas_sol.py;
```

## Output

The resulting CSV files _apache_beam_output.csv_ and _pandas_output.csv_ can be found in outputs directory. If we run scripts multiple time, it will overwrite the output files.


## Screenshots
Beam output: 
![beam](./rbeam.png)

pandas outpu:
![pandas](./rpandas.png)

