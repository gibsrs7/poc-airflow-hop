FROM apache/hop:2.10.0

USER root

# 1. Instala utilitários
RUN apk add --no-cache bash curl

# 2. LIMPEZA: Remove qualquer jar antigo da Oracle que possa ter vindo na imagem ou cache
RUN rm -f /opt/hop/lib/ojdbc*.jar

# 3. COPIA: Copia APENAS o ojdbc11.jar
# (Certifique-se que APENAS esse arquivo está na pasta do seu PC, ou edite o nome aqui)
COPY ojdbc11.jar /opt/hop/lib/

# 4. PERMISSÕES: Garante que todo mundo pode ler (chmod) e que pertence ao grupo root
RUN chmod 777 /opt/hop/lib/ojdbc11.jar

WORKDIR /files

