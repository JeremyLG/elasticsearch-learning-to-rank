# Commandes que j'ai découvert lors de cet exercice

## Docker

Je pars de zéro donc forcément tout est un peu nouveau

### Installation de docker en local sur ubuntu 18.04

```bash
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt update
apt-cache policy docker-ce
sudo apt install docker-ce
sudo systemctl status docker
sudo usermod -aG docker ${USER}
su - ${USER}
sudo apt install docker-compose
```

### Docker commands

```bash
docker-compose up
docker ps
docker-compose exec comet-usecase bash
```

## Jupyter Lab

Pour avoir la possibilité de find & replace dans la version LAB de Jupyter, il faut installer la prerelease : **jupyterlab==1.0.0a3**

Pour supprimer la demande de token et de password lors de la création du LAB :
```bash
jupyter lab --ip=0.0.0.0 --allow-root --NotebookApp.token='' --NotebookApp.password=''
```
