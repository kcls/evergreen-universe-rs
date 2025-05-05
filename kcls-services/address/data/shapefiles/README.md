# Render All Shapfiles

## Setup Python Env

```bash
sudo apt install python3-pip                                                   
mkdir $HOME/.venv                                                        
python3 -m venv $HOME/.venv                                              
export PATH="$HOME/.venv/bin:$PATH" # also add to ~/.bashrc              
pip install pyogrio geopandas pyogrio requests PyQt5
python3 ./render-all-shapefiles.py -o shapefiles-map.png
```


