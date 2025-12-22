FROM apache/hop:2.10.0

USER root

# 1. Instala utilitários
RUN apk add --no-cache bash curl

# 2. LIMPEZA TOTAL: Remove qualquer vestígio de driver antigo
RUN rm -f /opt/hop/lib/ojdbc*.jar
RUN rm -f /opt/hop/plugins/databases/oracle/lib/ojdbc*.jar

# 3. ESTRATÉGIA A: Pasta Isolada (Para a variável de ambiente)
RUN mkdir -p /lib-ext
COPY ojdbc11.jar /lib-ext/

# 4. ESTRATÉGIA B: Pasta do Plugin (Método Nativo do Hop)
# O Hop costuma olhar automaticamente dentro de plugins/databases/.../lib
RUN mkdir -p /opt/hop/plugins/databases/oracle/lib
COPY ojdbc11.jar /opt/hop/plugins/databases/oracle/lib/

# 5. PERMISSÕES: Liberar tudo para evitar erro de leitura
RUN chmod -R 777 /lib-ext
RUN chmod -R 777 /opt/hop/plugins/databases/oracle/lib

WORKDIR /files

