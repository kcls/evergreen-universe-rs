# Library of District Shapefiles

## Setup Python Env

```bash
sudo apt install python3-pip                                                   
mkdir $HOME/.python-env                                                        
python3 -m venv $HOME/.python-env                                              
export PATH="$HOME/.python-env/bin:$PATH" # also add to ~/.bashrc              
pip install pyogrio geopandas pyogrio requests                                 
python3 ./generate_district_shapefiles.py  
```

## Regenerating Files

```bash
python3 ./generate_district_shapefiles.py  
```

