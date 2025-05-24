FROM python:3.9.9-bullseye

WORKDIR /workdir

COPY requirements.txt .

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --upgrade pip

COPY app_enriched.py .

# With this we override the file that was copied into the Docker image
ENTRYPOINT ["streamlit", "run", "app_enriched.py"]  