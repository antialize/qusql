ARG builder
FROM $builder
WORKDIR /py
RUN yum -y install gcc libffi-devel
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
COPY mysql_type_plugin/ mysql_type_plugin/
COPY src/ src/
COPY pyproject.toml Cargo.lock Cargo.toml README.md ./
ARG pyver
ARG pym
RUN which linux32 && LINUX32=linux32 ; PATH=~/.cargo/bin:$PATH $LINUX32 /opt/python/cp$pyver-cp${pyver}$pym/bin/python -m pip wheel -e .
RUN find -maxdepth 1 -type f -name '*.whl' -exec bash -c 'auditwheel repair "$1" -w dist/ && rm "$1"' -- {} \;
