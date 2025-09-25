from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Boolean,
    ForeignKey,
    Float,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


## 1. Tabela Principal: processos
class Processo(Base):
    __tablename__ = "processos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    numero_cnj = Column(String, unique=True, nullable=False)
    titulo_polo_ativo = Column(String)
    titulo_polo_passivo = Column(String)
    ano_inicio = Column(Integer)
    data_inicio = Column(Date)
    estado_origem = Column(String)
    data_ultima_movimentacao = Column(Date)
    quantidade_movimentacoes = Column(Integer)
    fontes_tribunais_estao_arquivadas = Column(Boolean)
    tempo_desde_ultima_verificacao = Column(String)
    data_ultima_verificacao = Column(DateTime(timezone=True))

    unidade_origem_nome = Column(String)
    unidade_origem_cidade = Column(String)
    unidade_origem_estado = Column(String)
    unidade_origem_tribunal_sigla = Column(String)

    # Relações existentes
    processos_relacionados = relationship("ProcessoRelacionado", back_populates="processo")
    fontes = relationship("Fonte", back_populates="processo")

    # Nova relação com DadosPrecatorio (um-para-um)
    dados_precatorios = relationship("DadosPrecatorio", back_populates="processo", uselist=False)


## 2. Tabela para processos_relacionados
class ProcessoRelacionado(Base):
    __tablename__ = "processos_relacionados"
    id = Column(Integer, primary_key=True, autoincrement=True)
    processo_id = Column(Integer, ForeignKey("processos.id"))
    numero = Column(String)
    
    processo = relationship("Processo", back_populates="processos_relacionados")


## 3. Fonte
class Fonte(Base):
    __tablename__ = "fontes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    processo_id = Column(Integer, ForeignKey("processos.id"))
    fonte_id = Column(Integer)
    processo_fonte_id = Column(Integer)
    descricao = Column(String)
    nome = Column(String)
    sigla = Column(String)
    tipo = Column(String)
    data_inicio = Column(Date)
    data_ultima_movimentacao = Column(Date)
    segredo_justica = Column(Boolean)
    arquivado = Column(Boolean)
    status_predito = Column(String)
    grau = Column(Integer)
    grau_formatado = Column(String)
    fisico = Column(Boolean)
    sistema = Column(String)
    url = Column(Text)
    quantidade_envolvidos = Column(Integer)
    data_ultima_verificacao = Column(DateTime(timezone=True))
    quantidade_movimentacoes = Column(Integer)
    outros_numeros = Column(JSONB)

    processo = relationship("Processo", back_populates="fontes")
    capa = relationship("Capa", back_populates="fonte", uselist=False)
    envolvidos = relationship("Envolvido", back_populates="fonte")
    audiencias = relationship("Audiencia", back_populates="fonte")


## 4. Capa
class Capa(Base):
    __tablename__ = "fontes_capas"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fonte_id = Column(Integer, ForeignKey("fontes.id"))
    classe = Column(String)
    assunto = Column(Text)
    area = Column(String)
    orgao_julgador = Column(String)
    situacao = Column(String)
    data_distribuicao = Column(Date)
    data_arquivamento = Column(Date)

    fonte = relationship("Fonte", back_populates="capa")
    valor_causa = relationship("ValorCausa", back_populates="capa", uselist=False)
    informacoes_complementares = relationship("InformacaoComplementar", back_populates="capa")
    
    assuntos_normalizados = Column(JSONB)
    assunto_principal_normalizado = Column(JSONB)


## 5. ValorCausa
class ValorCausa(Base):
    __tablename__ = "capa_valores_causa"
    id = Column(Integer, primary_key=True, autoincrement=True)
    capa_id = Column(Integer, ForeignKey("fontes_capas.id"))
    valor = Column(Float)
    moeda = Column(String)
    valor_formatado = Column(String)

    capa = relationship("Capa", back_populates="valor_causa")


## 6. InformacaoComplementar
class InformacaoComplementar(Base):
    __tablename__ = "capa_informacoes_complementares"
    id = Column(Integer, primary_key=True, autoincrement=True)
    capa_id = Column(Integer, ForeignKey("fontes_capas.id"))
    tipo = Column(String)
    valor = Column(Text)

    capa = relationship("Capa", back_populates="informacoes_complementares")


## 7. Envolvido
class Envolvido(Base):
    __tablename__ = "fontes_envolvidos"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fonte_id = Column(Integer, ForeignKey("fontes.id"))
    nome = Column(String)
    quantidade_processos = Column(Integer)
    tipo_pessoa = Column(String)
    prefixo = Column(String)
    sufixo = Column(String)
    tipo = Column(String)
    tipo_normalizado = Column(String)
    polo = Column(String)
    cpf = Column(String)
    cnpj = Column(String)

    fonte = relationship("Fonte", back_populates="envolvidos")
    advogados = relationship("Advogado", back_populates="envolvido")


## 8. Advogado
class Advogado(Base):
    __tablename__ = "envolvidos_advogados"
    id = Column(Integer, primary_key=True, autoincrement=True)
    envolvido_id = Column(Integer, ForeignKey("fontes_envolvidos.id"))
    nome = Column(String)
    quantidade_processos = Column(Integer)
    tipo_pessoa = Column(String)
    prefixo = Column(String)
    sufixo = Column(String)
    tipo = Column(String)
    tipo_normalizado = Column(String)
    polo = Column(String)
    cpf = Column(String)
    cnpj = Column(String)

    envolvido = relationship("Envolvido", back_populates="advogados")
    oabs = relationship("OAB", back_populates="advogado")


## 9. OAB
class OAB(Base):
    __tablename__ = "advogados_oabs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    advogado_id = Column(Integer, ForeignKey("envolvidos_advogados.id"))
    uf = Column(String)
    tipo = Column(String)
    numero = Column(Integer)

    advogado = relationship("Advogado", back_populates="oabs")


## 10. Audiencia
class Audiencia(Base):
    __tablename__ = "audiencias"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fonte_id = Column(Integer, ForeignKey("fontes.id"), nullable=False)
    data_audiencia = Column(Date)
    descricao = Column(String)
    json_completo = Column(JSONB)

    fonte = relationship("Fonte", back_populates="audiencias")


## 11. Nova tabela: DadosPrecatorio
class DadosPrecatorio(Base):
    __tablename__ = "dados_precatorios"
    id = Column(Integer, primary_key=True, autoincrement=True)
    processo_id = Column(Integer, ForeignKey("processos.id"), unique=True, nullable=False)
    tipo_regime = Column(String)
    ano_orcamentario = Column(Integer)
    natureza_precatorio = Column(String)
    valor_deferido = Column(Float)
    data_base_calculo = Column(Date)
    data_expedicao = Column(Date)

    processo = relationship("Processo", back_populates="dados_precatorios", uselist=False)
