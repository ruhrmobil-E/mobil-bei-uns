# mobil-bei-uns

Das Projekt mobilität finden ist auf [mobil-bei-uns.de](https://mobil-bei-uns.de/) zu finden.

# Eingesetzte Software

mobilität ist eine WSGI-Applikation mit folgenden Komponenten:
- Flask
- Python
- MongoDB
- Gulp
- Bower
- npm

Die Applikation wird via gunicorn, Nginx und supervisord bereitgestellt.

# Zusätzliche Importmodule

Um weitere Daten in die Plattform zu integrieren, muss ein weiteres Importmodul im Ordner /sync angelegt werden.