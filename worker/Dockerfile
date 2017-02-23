FROM gcr.io/google_appengine/python
ENV https_proxy "http://proxy.ir.intel.com:911"
ENV http_proxy "http://proxy.ir.intel.com:911"
RUN echo "Acquire::http::Proxy \"http://proxy.ir.intel.com:911\";" > /etc/apt/apt.conf.d/999proxy
RUN apt-get -y update && apt-get install -y libav-tools
RUN virtualenv /env
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
ADD . /app
CMD honcho start -f /app/procfile worker monitor
ENV https_proxy ""
ENV http_proxy ""

