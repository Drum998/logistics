FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ram_dashboard /app/ram_dashboard

EXPOSE 8000

ENV FLASK_APP=ram_dashboard.app
ENV FLASK_ENV=production

CMD ["gunicorn", "-b", "0.0.0.0:8000", "--timeout", "600", "ram_dashboard.app:app"]

