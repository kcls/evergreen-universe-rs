# Library of District Shapefiles

## Setup Python Env

```bash
sudo apt install python3-pip                                                   
mkdir $HOME/.venv                                                        
python3 -m venv $HOME/.venv                                              
export PATH="$HOME/.venv/bin:$PATH" # also add to ~/.bashrc              
pip install pyogrio geopandas pyogrio requests                                 
python3 ./generate_district_shapefiles.py  
```

## Regenerating Files

```bash
python3 ./generate_district_shapefiles.py  
```

