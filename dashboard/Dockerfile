FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    dash==2.17.1 \
    plotly==5.22.0 \
    pandas==2.2.2 \
    SQLAlchemy==2.0.30 \
    psycopg2-binary==2.9.9
COPY app.py .
EXPOSE 8050
CMD ["python", "app.py"]
