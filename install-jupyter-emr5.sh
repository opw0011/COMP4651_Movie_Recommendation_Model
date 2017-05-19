#!/bin/bash
set -x -e

# AWS EMR bootstrap script 
# for installing Jupyter notebook on AWS EMR 5+
#
# 2016-11-04 - Tom Zeng tomzeng@amazon.com, initial version
# 2016-11-20 - Tom Zeng, add JupyterHub
# 2016-12-01 - Tom Zeng, add s3 support and cached install
# 2016-12-03 - Tom Zeng, use puppet to install/run services
# 2016-12-06 - Tom Zeng, switch to s3fs for S3 support since s3nb is not fully working
#
# Usage:
# --r - install the IRKernel for R
# --toree - install the Apache Toree kernel that supports Scala, PySpark, SQL, SparkR for Apache Spark
# --interpreters - specify Apache Toree interpreters, default is all: "Scala,SQL,PySpark,SparkR"
# --julia - install the IJulia kernel for Julia
# --ruby - install the iRuby kernel for Ruby
# --torch - intall the iTorch kernel for Torch
# --ds-packages - install the Python Data Science related packages (scikit-learn pandas numpy numexpr statsmodels seaborn)
# --ml-packages - install the Python Machine Learning related packages (theano keras tensorflow)
# --python-packages - install specific python packages e.g. "ggplot nilean"
# --port - set the port for Jupyter notebook, default is 8888
# --password - set the password for Jupyter notebook
# --localhost-only - restrict jupyter to listen on localhost only, default to listen on all ip addresses for the instance
# --jupyterhub - install JupyterHub
# --jupyterhub-port - set the port for JuputerHub, default is 8000
# --notebook-dir - specify notebook folder, this could be a local directory or a S3 bucket
# --cached-install - use some cached dependency artifacts on s3 to speed up installation
# --ssl - enable ssl, make sure to use your own cert and key files to get rid of the warning
# --copy-samples - copy sample notebooks to samples sub folder under the notebook folder
# --spark-opts - user supplied Spark options to override the default SPARK_OPTS:
#                --packages com.databricks:spark-csv_2.11:1.5.0com.databricks:spark-avro_2.11:3.0.0,org.elasticsearch:elasticsearch-spark_2.11:2.4.0
# --s3fs - use s3fs instead of s3bn for storing notebooks on s3, s3fs could cause slowness if the s3 bucket has lots of file. On the other hand upload file and create folder do not work with s3nb

# check for master node
IS_MASTER=false
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

# error message
error_msg ()
{
  echo 1>&2 "Error: $1"
}

# some defaults
RUBY_KERNEL=false
R_KERNEL=false
JULIA_KERNEL=false
TOREE_KERNEL=false
TORCH_KERNEL=false
DS_PACKAGES=false
ML_PACKAGES=false
PYTHON_PACKAGES=""
RUN_AS_STEP=false
NOTEBOOK_DIR=""
NOTEBOOK_DIR_S3=false
JUPYTER_PORT=8888
JUPYTER_PASSWORD=""
JUPYTER_LOCALHOST_ONLY=false
PYTHON3=false
GPU=false
CPU_GPU="cpu"
JUPYTER_HUB=false
JUPYTER_HUB_PORT=8000
JUPYTER_HUB_IP="*"
USE_CACHED_DEPS=true
INTERPRETERS="Scala,SQL,PySpark,SparkR"
R_REPOS_LOCAL="file:////mnt/miniCRAN"
R_REPOS_REMOTE="http://cran.rstudio.com"
R_REPOS=$R_REPOS_LOCAL
SSL=false
SSL_OPTS="--no-ssl"
COPY_SAMPES=false
USER_SPARK_OPTS=""
NOTEBOOK_DIR_S3_S3NB=true
JS_KERNEL=false

# get input parameters
while [ $# -gt 0 ]; do
    case "$1" in
    --r)
      R_KERNEL=true
      ;;
    --julia)
      JULIA_KERNEL=true
      ;;
    --toree)
      TOREE_KERNEL=true
      ;;
    --torch)
      TORCH_KERNEL=true
      ;;
    --javascript)
      JS_KERNEL=true
      ;;
    --ds-packages)
      DS_PACKAGES=true
      ;;
    --ml-packages)
      ML_PACKAGES=true
      ;;
    --python-packages)
      shift
      PYTHON_PACKAGES=$1
      ;;
    --ruby)
      RUBY_KERNEL=true
      ;;
    --gpu)
      GPU=true
      CPU_GPU="gpu"
      ;;
    --run-as-step)
      RUN_AS_STEP=true
      ;;
    --port)
      shift
      JUPYTER_PORT=$1
      ;;
    --password)
      shift
      JUPYTER_PASSWORD=$1
      ;;
    --localhost-only)
      JUPYTER_LOCALHOST_ONLY=true
      JUPYTER_HUB_IP=""
      ;;
    --jupyterhub)
      JUPYTER_HUB=true
      #PYTHON3=true
      ;;
    --jupyterhub-port)
      shift
      JUPYTER_HUB_PORT=$1
      ;;
    --notebook-dir)
      shift
      NOTEBOOK_DIR=$1
      ;;
    --copy-samples)
      COPY_SAMPLES=true
      ;;
    --toree-interpreters)
      shift
      INTERPRETERS=$1
      ;;
    --cached-install)
      USE_CACHED_DEPS=true
      R_REPOS=$R_REPOS_LOCAL
      ;;
    --no-cached-install)
      USE_CACHED_DEPS=false
      R_REPOS=$R_REPOS_REMOTE
      ;;
    --ssl)
      SSL=true
      ;;
    --spark-opts)
      shift
      USER_SPARK_OPTS=$1
      ;;
    --s3fs)
      NOTEBOOK_DIR_S3_S3NB=false
      ;;
    -*)
      # do not exit out, just note failure
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
    esac
    shift
done

sudo bash -c 'echo "fs.file-max = 25129162" >> /etc/sysctl.conf'
sudo sysctl -p /etc/sysctl.conf
sudo bash -c 'echo "* soft    nofile          1048576" >> /etc/security/limits.conf'
sudo bash -c 'echo "* hard    nofile          1048576" >> /etc/security/limits.conf'
sudo bash -c 'echo "session    required   pam_limits.so" >> /etc/pam.d/su'

sudo puppet module install spantree-upstart

RELEASE=$(cat /etc/system-release)
REL_NUM=$(ruby -e "puts '$RELEASE'.split.last")

if [ "$USE_CACHED_DEPS" = true ]; then
  cd /mnt
  aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/jupyter-deps.zip .
  aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/miniCRAN.zip .
  aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/yum-rpm-cache-$REL_NUM.zip .
  unzip jupyter-deps.zip
  unzip miniCRAN.zip
  unzip yum-rpm-cache-$REL_NUM.zip
  rm jupyter-deps.zip
  rm miniCRAN.zip
  rm yum-rpm-cache-$REL_NUM.zip
fi

# move /usr/lib to /mnt/usr-moved/lib to avoid running out of space on /
if [ ! -d /mnt/usr-moved ]; then
  sudo mkdir /mnt/usr-moved
  sudo mv /usr/local /mnt/usr-moved/
  sudo ln -s /mnt/usr-moved/local /usr/
  sudo mv /usr/share /mnt/usr-moved/
  sudo ln -s /mnt/usr-moved/share /usr/
fi

export MAKE='make -j 8'

if [ "$USE_CACHED_DEPS" = true ]; then
  sudo yum install --skip-broken -y /mnt/yum-rpm-cache/*
else
  sudo yum install -y xorg-x11-xauth.x86_64 xorg-x11-server-utils.x86_64 xterm libXt libX11-devel libXt-devel libcurl-devel git graphviz cyrus-sasl cyrus-sasl-devel readline readline-devel
  sudo yum install --enablerepo=epel -y nodejs npm zeromq3 zeromq3-devel
  sudo yum install -y gcc-c++ patch zlib zlib-devel
  sudo  yum install -y libyaml-devel libffi-devel openssl-devel make
  sudo yum install -y bzip2 autoconf automake libtool bison iconv-devel sqlite-devel
fi

export NODE_PATH='/usr/lib/node_modules'
if [ "$JS_KERNEL" = true ]; then
  sudo npm cache clean -f
  sudo npm install -g npm
  sudo npm install -g n
  sudo n stable
fi

cd /mnt
if [ "$PYTHON3" = true ]; then
  export PYSPARK_PYTHON="python34"
  sudo ln -sf /usr/bin/python3.4 /usr/bin/python
  sudo ln -sf /usr/bin/pip-3.4 /usr/bin/pip
else
  sudo python -m pip install --upgrade pip
  sudo ln -sf /usr/local/bin/pip2.7 /usr/bin/pip
fi
TF_BINARY_URL_PY3="https://storage.googleapis.com/tensorflow/linux/$CPU_GPU/tensorflow-0.12.0-cp34-cp34m-linux_x86_64.whl"
TF_BINARY_URL="https://storage.googleapis.com/tensorflow/linux/$CPU_GPU/tensorflow-0.12.0-cp27-none-linux_x86_64.whl"


sudo python3 -m pip install -U jupyter
#sudo python3 -m pip install -U matplotlib seaborn cython networkx
sudo python -m pip install -U matplotlib seaborn cython networkx findspark
#sudo python3 -m pip install -U mrjob pyhive sasl thrift thrift-sasl snakebite
sudo python -m pip install -U mrjob pyhive sasl thrift thrift-sasl snakebite

sudo ln -sf /usr/local/bin/ipython /usr/bin/
sudo ln -sf /usr/local/bin/jupyter /usr/bin/

if [ "$DS_PACKAGES" = true ]; then
  # Python
  sudo python -m pip install -U scikit-learn pandas numpy numexpr statsmodels scipy
  #sudo python3 -m pip install -U scikit-learn pandas numpy numexpr statsmodels scipy
  # Javascript
  if [ "$JS_KERNEL" = true ]; then
    sudo npm install -g --unsafe-perm stats-analysis decision-tree machine_learning limdu synaptic node-svm lda brain.js scikit-node
  fi
fi

if [ "$ML_PACKAGES" = true ]; then
  sudo python -m pip install -U theano
  sudo python -m pip install -U keras
  sudo python -m pip install -U $TF_BINARY_URL
  #sudo python3 -m pip install -U theano
  #sudo python3 -m pip install -U keras
  #sudo python3 -m pip install -U $TF_BINARY_URL_PY3
fi

if [ ! "$PYTHON_PACKAGES" = "" ]; then
  sudo python -m pip install -U $PYTHON_PACKAGES || true
  #sudo python3 -m pip install -U $PYTHON_PACKAGES || true
fi

if [ "$JULIA_KERNEL" = true ]; then
  # Julia install
  cd /mnt
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    wget https://julialang.s3.amazonaws.com/bin/linux/x64/0.5/julia-0.5.0-linux-x86_64.tar.gz
    tar xvfz julia-0.5.0-linux-x86_64.tar.gz
  fi
  cd julia-3c9d75391c
  sudo cp -pr bin/* /usr/bin/
  sudo cp -pr lib/* /usr/lib/
  #sudo cp -pr libexec/* /usr/libexec/
  sudo cp -pr share/* /usr/share/
  sudo cp -pr include/* /usr/include/
fi

#echo ". /mnt/ipython-env/bin/activate" >> ~/.bashrc

# only run below on master instance
if [ "$IS_MASTER" = true ]; then

if [ "$RUBY_KERNEL" = true ]; then
  cd /mnt
  if [ "$USE_CACHED_DEPS" != true ]; then
    wget http://ftp.ruby-lang.org/pub/ruby/2.1/ruby-2.1.8.tar.gz 
    tar xvzf ruby-2.1.8.tar.gz
  fi
  cd ruby-2.1.8
  ./configure --prefix=/usr
  make
  sudo make install
  #sudo gem install puppet -v=3.7.4 -N
  sudo gem install rbczmq iruby -N
  sudo gem install presto-client -N
  sudo iruby register
  sudo cp -pr /root/.ipython/kernels/ruby /usr/local/share/jupyter/kernels/
  iruby register
fi

mkdir -p ~/.jupyter
touch ls ~/.jupyter/jupyter_notebook_config.py

sed -i '/c.NotebookApp.open_browser/d' ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py

if [ ! "$JUPYTER_LOCALHOST_ONLY" = true ]; then
sed -i '/c.NotebookApp.ip/d' ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip='*'" >> ~/.jupyter/jupyter_notebook_config.py
fi

sed -i '/c.NotebookApp.port/d' ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = $JUPYTER_PORT" >> ~/.jupyter/jupyter_notebook_config.py

if [ ! "$JUPYTER_PASSWORD" = "" ]; then
  sed -i '/c.NotebookApp.password/d' ~/.jupyter/jupyter_notebook_config.py
  HASHED_PASSWORD=$(python3 -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))")
  echo "c.NotebookApp.password = u'$HASHED_PASSWORD'" >> ~/.jupyter/jupyter_notebook_config.py
else
  sed -i '/c.NotebookApp.token/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.token = u''" >> ~/.jupyter/jupyter_notebook_config.py
fi

echo "c.Authenticator.admin_users = {'hadoop'}" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.LocalAuthenticator.create_system_users = True" >> ~/.jupyter/jupyter_notebook_config.py

if [ "$SSL" = true ]; then
  #NOTE - replace server.cert and server.key with your own cert and key files
  CERT=/usr/local/etc/server.cert
  KEY=/usr/local/etc/server.key
  sudo openssl req -x509 -nodes -days 3650 -newkey rsa:1024 -keyout $KEY -out $CERT -subj "/C=US/ST=Washington/L=Seattle/O=JupyterCert/CN=JupyterCert"
  
  # the following works for Jupyter but will fail JupyterHub, use options for both instead
  #echo "c.NotebookApp.certfile = u'/usr/local/etc/server.cert'" >> ~/.jupyter/jupyter_notebook_config.py
  #echo "c.NotebookApp.keyfile = u'/usr/local/etc/server.key'" >> ~/.jupyter/jupyter_notebook_config.py

  SSL_OPTS_JUPYTER="--keyfile=/usr/local/etc/server.key --certfile=/usr/local/etc/server.cert"
  SSL_OPTS_JUPYTERHUB="--ssl-key=/usr/local/etc/server.key --ssl-cert=/usr/local/etc/server.cert"
fi

# install default kernels
sudo python3 -m pip install -U notebook ipykernel
sudo python3 -m ipykernel install
sudo python -m pip install -U notebook ipykernel
sudo python -m ipykernel install
sudo python3 -m pip install -U metakernel
#sudo python3 -m pip install -U gnuplot_kernel
#sudo python3 -m gnuplot_kernel install
#sudo python -m pip install -U gnuplot_kernel
#sudo python -m gnuplot_kernel install
sudo python3 -m pip install -U bash_kernel
sudo python3 -m bash_kernel.install

# Javascript/CoffeeScript kernels
if [ "$JS_KERNEL" = true ]; then
  sudo npm install -g --unsafe-perm ijavascript d3 lodash plotly jp-coffeescript
  sudo ijs --ijs-install=global
  sudo jp-coffee --jp-install=global
fi
sudo python3 -m pip install -U jupyter_contrib_nbextensions
sudo python -m pip install -U jupyter_contrib_nbextensions
sudo jupyter contrib nbextension install --system
sudo python3 -m pip install -U jupyter_nbextensions_configurator
sudo python -m pip install -U jupyter_nbextensions_configurator
sudo jupyter nbextensions_configurator enable --system
sudo python3 -m pip install -U ipywidgets
sudo python -m pip install -U ipywidgets
sudo jupyter nbextension enable --py --sys-prefix widgetsnbextension

sudo python3 -m pip install -U pyeda # only work for python3
sudo python -m pip install -U gvmagic py_d3
sudo python -m pip install -U ipython-sql rpy2

if [ "$JULIA_KERNEL" = true ]; then
  julia -e 'Pkg.add("IJulia")'
  julia -e 'Pkg.add("RDatasets");Pkg.add("Gadfly");Pkg.add("DataFrames");Pkg.add("PyPlot")'
  # Julia's Spark support does not support Spark on Yarn yet
  # install mvn
  #cd /mnt
  #aws s3 cp s3://tomzeng/maven/apache-maven-3.3.9-bin.tar.gz .
  #tar xvfz apache-maven-3.3.9-bin.tar.gz
  #sudo mv apache-maven-3.3.9 /opt/maven
  #sudo ln -s /opt/maven/bin/mvn /usr/bin/mvn
  # install Spark for Julia
  #julia -e 'Pkg.clone("https://github.com/dfdx/Spark.jl"); Pkg.build("Spark"); Pkg.checkout("JavaCall")'
fi

# iTorch depends on Torch which is installed with --ml-packages
if [ "$TORCH_KERNEL" = true ]; then
  set +e # workaround for the lengthy torch install-deps, esp when other background process are also running yum
  cd /mnt
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    git clone https://github.com/torch/distro.git torch-distro
  fi
  cd torch-distro
  ./install-deps
  ./install.sh -b
  export PATH=$PATH:/mnt/torch-distro/install/bin
  source ~/.profile
  luarocks install lzmq
  luarocks install gnuplot
  cd /mnt
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    git clone https://github.com/facebook/iTorch.git
  fi
  cd iTorch
  luarocks make
  sudo cp -pr ~/.ipython/kernels/itorch /usr/local/share/jupyter/kernels/
  set -e
fi

if [ "$R_KERNEL" = true ] || [ "$TOREE_KERNEL" = true ]; then
  if [ ! -f /tmp/Renvextra ]; then # check if the rstudio ba was run, it does this already 
   sudo sed -i 's/make/make -j 8/g' /usr/lib64/R/etc/Renviron
  fi
  sudo R --no-save << R_SCRIPT
  install.packages(c('RJSONIO', 'itertools', 'digest', 'Rcpp', 'functional', 'httr', 'plyr', 'stringr', 'reshape2', 'caTools', 'rJava', 'devtools', 'DBI', 'ggplot2', 'dplyr', 'R.methodsS3', 'Hmisc', 'memoise', 'rjson'), repos="$R_REPOS", quiet = TRUE)
R_SCRIPT
  # For the Granger Causuality example deps
  sudo R --no-save << R_SCRIPT
  library(devtools)
  install.packages(c('fUnitRoots','vars','aod','tseries'), repos="$R_REPOS", quiet = TRUE)
R_SCRIPT
fi

# IRKernal setup 
if [ "$R_KERNEL" = true ]; then
  sudo R --no-save << R_SCRIPT
  install.packages(c("curl", "httr", "devtools", "repr", "IRdisplay", "evaluate", "crayon", "pbdZMQ", "uuid", "digest", "e1071", "party"), repos="$R_REPOS", quiet = TRUE)
  devtools::install_github("IRkernel/IRkernel")
  IRkernel::installspec(user = FALSE)
R_SCRIPT
fi

if [ ! "$NOTEBOOK_DIR" = "" ]; then
  NOTEBOOK_DIR="${NOTEBOOK_DIR%/}/" # remove trailing / if exists then add /
  if [[ "$NOTEBOOK_DIR" == s3://* ]]; then
    NOTEBOOK_DIR_S3=true
    # the s3nb does not fully working yet(upload and createe folder not working)
    if [ "$NOTEBOOK_DIR_S3_S3NB" = true ]; then
      cd /mnt
      if [ ! "$USE_CACHED_DEPS" = true ]; then
        git clone https://github.com/tomz/s3nb.git
      fi
      cd s3nb
      #sudo python -m pip install -U entrypoints
      #sudo python setup.py install
      #if [ "$JUPYTER_HUB" = true ]; then
        sudo python3 -m pip install -U entrypoints
        sudo python3 setup.py install
      #fi

      echo "c.NotebookApp.contents_manager_class = 's3nb.S3ContentsManager'" >> ~/.jupyter/jupyter_notebook_config.py
      echo "c.S3ContentsManager.checkpoints_kwargs = {'root_dir': '~/.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
      # if just bucket with no subfolder, a trailing / is required, otherwise s3nb will break
      echo "c.S3ContentsManager.s3_base_uri = '$NOTEBOOK_DIR'" >> ~/.jupyter/jupyter_notebook_config.py
      #echo "c.S3ContentsManager.s3_base_uri = '${NOTEBOOK_DIR_S3%/}/%U'" >> ~/.jupyter/jupyter_notebook_config.py
      #echo "c.Spawner.default_url = '${NOTEBOOK_DIR_S3%/}/%U'" >> ~/.jupyter/jupyter_notebook_config.py
      #echo "c.Spawner.notebook_dir = '/%U'" >> ~/.jupyter/jupyter_notebook_config.py 
    else
      BUCKET=$(ruby -e "nb_dir='$NOTEBOOK_DIR';puts nb_dir.split('//')[1].split('/')[0]")
      FOLDER=$(ruby -e "nb_dir='$NOTEBOOK_DIR';puts nb_dir.split('//')[1].split('/')[1..-1].join('/')")
      if [ "$USE_CACHED_DEPS" != true ]; then
        sudo yum install -y automake fuse fuse-devel libxml2-devel
      fi
      cd /mnt
      git clone https://github.com/s3fs-fuse/s3fs-fuse.git
      cd s3fs-fuse/
      ls -alrt
      ./autogen.sh
      ./configure
      make
      sudo make install
      sudo su -c 'echo user_allow_other >> /etc/fuse.conf'
      mkdir -p /mnt/s3fs-cache
      mkdir -p /mnt/$BUCKET
      #/usr/local/bin/s3fs -o allow_other -o iam_role=auto -o umask=0 $BUCKET /mnt/$BUCKET
      # -o nodnscache -o nosscache -o parallel_count=20  -o multipart_size=50
      /usr/local/bin/s3fs -o allow_other -o iam_role=auto -o umask=0 -o url=https://s3.amazonaws.com  -o no_check_certificate -o enable_noobj_cache -o use_cache=/mnt/s3fs-cache $BUCKET /mnt/$BUCKET
      #/usr/local/bin/s3fs -o allow_other -o iam_role=auto -o umask=0 -o use_cache=/mnt/s3fs-cache $BUCKET /mnt/$BUCKET
      echo "c.NotebookApp.notebook_dir = '/mnt/$BUCKET/$FOLDER'" >> ~/.jupyter/jupyter_notebook_config.py
      echo "c.ContentsManager.checkpoints_kwargs = {'root_dir': '.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
    fi
  else
    echo "c.NotebookApp.notebook_dir = '$NOTEBOOK_DIR'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.ContentsManager.checkpoints_kwargs = {'root_dir': '.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
  fi
fi

if [ "$COPY_SAMPLES" = true ]; then
  if [ "$NOTEBOOK_DIR_S3" = true ]; then
    aws s3 sync s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/notebooks/ ${NOTEBOOK_DIR}samples/ || true
  else
    cd ~
    if [ ! "$NOTEBOOK_DIR" = "" ]; then
      mkdir -p ${NOTEBOOK_DIR}samples || true
      aws s3 sync s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/notebooks/ ${NOTEBOOK_DIR}samples || true
    else
      aws s3 sync s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/notebooks/ samples/ || true
    fi
  fi
fi

background_install_proc() {
  # wait SparkR file to show up
  while [ ! -d /usr/lib/spark/R/lib/SparkR ]
  do
    sleep 10
  done
  echo "Found /usr/lib/spark/R/lib/SparkR"
  
  if [ ! -f /tmp/Renvextra ]; then # check if the rstudio BA maybe already done this
    cat << 'EOF' > /tmp/Renvextra
JAVA_HOME="/etc/alternatives/jre"
HADOOP_HOME_WARN_SUPPRESS="true"
HADOOP_HOME="/usr/lib/hadoop"
HADOOP_PREFIX="/usr/lib/hadoop"
HADOOP_MAPRED_HOME="/usr/lib/hadoop-mapreduce"
HADOOP_YARN_HOME="/usr/lib/hadoop-yarn"
HADOOP_COMMON_HOME="/usr/lib/hadoop"
HADOOP_HDFS_HOME="/usr/lib/hadoop-hdfs"
HADOOP_CONF_DIR="/usr/lib/hadoop/etc/hadoop/"
YARN_CONF_DIR="/usr/lib/hadoop/etc/hadoop/"
YARN_HOME="/usr/lib/hadoop-yarn"
HIVE_HOME="/usr/lib/hive"
HIVE_CONF_DIR="/usr/lib/hive/conf"
HBASE_HOME="/usr/lib/hbase"
HBASE_CONF_DIR="/usr/lib/hbase/conf"
SPARK_HOME="/usr/lib/spark"
SPARK_CONF_DIR="/usr/lib/spark/conf"
PATH=${PWD}:${PATH}
EOF

  if [ "$PYSPARK_PYTHON" = "python34" ]; then
    cat << 'EOF' >> /tmp/Renvextra
PYSPARK_PYTHON="python34"
EOF
  fi

  cat /tmp/Renvextra | sudo tee -a /usr/lib64/R/etc/Renviron

  sudo mkdir -p /mnt/spark
  sudo chmod a+rwx /mnt/spark
  if [ -d /mnt1 ]; then
    sudo mkdir -p /mnt1/spark
    sudo chmod a+rwx /mnt1/spark
  fi

  sudo R --no-save << R_SCRIPT
  library(devtools)
  install.packages(c('sparklyr', 'nycflights13', 'Lahman', 'data.table'), repos="$R_REPOS", quiet = TRUE)
R_SCRIPT

  set +e # workaround for if SparkR is already installed by other BA
  # install SparkR and SparklyR for R - toree ifself does not need this
  sudo R --no-save << R_SCRIPT
  library(devtools)
  install('/usr/lib/spark/R/lib/SparkR')
R_SCRIPT
  set -e

  fi # end if -f /tmp/Renvextra

  sudo python -m pip install /mnt/incubator-toree/dist/toree-pip
  #sudo ln -sf /usr/local/bin/jupyter-toree /usr/bin/
  export SPARK_HOME="/usr/lib/spark/"
  SPARK_PACKAGES="com.databricks:spark-csv_2.11:1.5.0,com.databricks:spark-avro_2.11:3.0.0,org.elasticsearch:elasticsearch-spark_2.11:2.4.0"
  if [ "$USER_SPARK_OPTS" = "" ]; then
    SPARK_OPTS="--packages $SPARK_PACKAGES"
  else
    SPARK_OPTS=$USER_SPARK_OPTS
    SPARK_PACKAGES=$(ruby -e "opts='$SPARK_OPTS'.split;pkgs=nil;opts.each_with_index{|o,i| pkgs=opts[i+1] if o.start_with?('--packages')};puts pkgs || '$SPARK_PACKAGES'")
  fi
  export SPARK_OPTS
  export SPARK_PACKAGES
  sudo jupyter toree install --interpreters=$INTERPRETERS --spark_home=$SPARK_HOME --spark_opts="$SPARK_OPTS"
  #sudo jupyter toree install --interpreters=Scala,PySpark,SQL,SparkR --spark_home=$SPARK_HOME --spark_opts=$SPARK_OPTS
  # NOTE - toree does not pick SPARK_OPTS, so use the following workaround until it's fixed
  
  while [ ! -f /etc/spark/conf/spark-defaults.conf ]
  do
    sleep 10
  done
  echo "Found /etc/spark/conf/spark-defaults.conf"
  if ! grep "spark.jars.packages" /etc/spark/conf/spark-defaults.conf; then
    sudo bash -c "echo 'spark.jars.packages              $SPARK_PACKAGES' >> /etc/spark/conf/spark-defaults.conf"
  fi
  echo "Starting Jupyter notebook via pyspark"
  cd ~
  #PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser" pyspark > /var/log/jupyter.log &
  sudo puppet apply << PUPPET_SCRIPT
  include 'upstart'
  upstart::job { 'jupyter':
      description    => 'Jupyter',
      respawn        => true,
      respawn_limit  => '0 10',
      start_on       => 'runlevel [2345]',
      stop_on        => 'runlevel [016]',
      console        => 'output',
      chdir          => '/home/hadoop',
      script           => '
      sudo su - hadoop > /var/log/jupyter.log 2>&1 <<BASH_SCRIPT
      export NODE_PATH="$NODE_PATH"
      export PYSPARK_DRIVER_PYTHON="jupyter"
      export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser $SSL_OPTS_JUPYTER --log-level=INFO"
      export NOTEBOOK_DIR="$NOTEBOOK_DIR"
      pyspark
BASH_SCRIPT
      ',
  }
PUPPET_SCRIPT

}

# apache toree install
if [ "$TOREE_KERNEL" = true ]; then
  echo "Running background process to install Apacke Toree"
  # spark 1.6
  #sudo pip install --pre toree
  #sudo jupyter toree install

  # spark 2.0
  cd /mnt
  if [ "$USE_CACHED_DEPS" != true ]; then
    curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
    sudo yum install docker sbt -y
  fi
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    git clone https://github.com/apache/incubator-toree.git
  fi
  cd incubator-toree/
  make -j8 dist
  make release || true # gettting the docker not running error, swallow it with || true
  if [ "$RUN_AS_STEP" = true ]; then
    background_install_proc
  else
    background_install_proc &
  fi
else
  echo "Starting Jupyter notebook"
  sudo puppet apply << PUPPET_SCRIPT
  include 'upstart'
  upstart::job { 'jupyter':
      description    => 'Jupyter',
      respawn        => true,
      respawn_limit  => '0 10',
      start_on       => 'runlevel [2345]',
      stop_on        => 'runlevel [016]',
      console        => 'output',
      chdir          => '/home/hadoop',
      env            => { 'NOTEBOOK_DIR' => '$NOTEBOOK_DIR', 'NODE_PATH' => '$NODE_PATH' },
      exec           => 'sudo su - hadoop -c "jupyter notebook --no-browser $SSL_OPTS_JUPYTER" > /var/log/jupyter.log 2>&1',
  }
PUPPET_SCRIPT
fi

if [ "$JUPYTER_HUB" = true ]; then
  # change the password of the hadoop user to JUPYTER_PASSWORD
  if [ ! "$JUPYTER_PASSWORD" = "" ]; then
    sudo sh -c "echo '$JUPYTER_PASSWORD' | passwd hadoop --stdin"
  fi
  sudo npm install -g --unsafe-perm configurable-http-proxy
  sudo python3 -m pip install jupyterhub #notebook ipykernel
  #sudo python3 -m ipykernel install
  sudo ln -sf /usr/local/bin/jupyterhub /usr/bin/
  sudo ln -sf /usr/local/bin/jupyterhub-singleuser /usr/bin/
  mkdir -p /mnt/jupyterhub
  cd /mnt/jupyterhub
  echo "Starting Jupyterhub"
  #sudo jupyterhub $SSL_OPTS_JUPYTERHUB --port=$JUPYTER_HUB_PORT --ip=$JUPYTER_HUB_IP --log-file=/var/log/jupyterhub.log --config ~/.jupyter/jupyter_notebook_config.py &
  sudo puppet apply << PUPPET_SCRIPT
  include 'upstart'
  upstart::job { 'jupyterhub':
      description    => 'JupyterHub',
      respawn        => true,
      respawn_limit  => '0 10',
      start_on       => 'runlevel [2345]',
      stop_on        => 'runlevel [016]',
      console        => 'output',
      chdir          => '/mnt/jupyterhub',
      env            => { 'NOTEBOOK_DIR' => '$NOTEBOOK_DIR', 'NODE_PATH' => '$NODE_PATH' },
      exec           => 'sudo /usr/bin/jupyterhub --pid-file=/var/run/jupyter.pid $SSL_OPTS_JUPYTERHUB --port=$JUPYTER_HUB_PORT --ip=$JUPYTER_HUB_IP --log-file=/var/log/jupyterhub.log --config /home/hadoop/.jupyter/jupyter_notebook_config.py'
  }
PUPPET_SCRIPT

fi

fi
echo "Bootstrap action finished"
