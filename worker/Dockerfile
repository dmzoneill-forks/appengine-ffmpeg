FROM gcr.io/google_appengine/python
ENV https_proxy "http://proxy.ir.intel.com:911"
ENV http_proxy "http://proxy.ir.intel.com:911"
RUN echo "Acquire::http::Proxy \"http://proxy.ir.intel.com:911\";" > /etc/apt/apt.conf.d/999proxy
RUN echo "deb http://www.deb-multimedia.org jessie main non-free" >> /etc/apt/sources.list
RUN echo "deb-src http://www.deb-multimedia.org jessie main non-free" >> /etc/apt/sources.list
RUN apt-get --allow-unauthenticated -y update
RUN apt-get --allow-unauthenticated -y install deb-multimedia-keyring
RUN apt-get --allow-unauthenticated -y update
RUN apt-get --allow-unauthenticated -y install libav-tools ffmpeg nmap vim 
RUN virtualenv /env
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
ADD . /app
CMD honcho start -f /app/procfile worker monitor
ENV https_proxy ""
ENV http_proxy ""

