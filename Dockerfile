FROM continuumio/miniconda3


RUN git clone https://github.com/tkasu/HadoopLearning.git app 
WORKDIR /app 

RUN conda env create -f py_environment.yml

ENV PATH /opt/conda/envs/hadoop-learning/bin:$PATH
RUN echo "source activate hadoop-learning" >> ~/.bashrc

WORKDIR /app/src/main/python
ENTRYPOINT ["python", "-u", "-m", "ncdc_analysis.cli.cluster_runner"]
CMD ["--help"]
