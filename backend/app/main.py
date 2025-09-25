import os
import pandas as pd
import chardet
from fastapi import BackgroundTasks, FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from app.worker import processar_lote
from app.database import AsyncSessionLocal
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from app.models import Processo, Fonte, Capa, Envolvido, InformacaoComplementar, Advogado, OAB, DadosPrecatorio
import openpyxl 
from datetime import datetime
import csv
import io
import logging
import re
import zipfile
from io import BytesIO

app = FastAPI(title="API de Processamento de Precatórios - RECALL")

# Configuração do CORS
origins = ["http://localhost:3000", "http://127.0.0.1:3000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pasta para arquivos temporários
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def delete_file(path: str):
    try:
        os.remove(path)
    except Exception as e:
        print(f"Erro ao deletar arquivo: {e}")


# Configuração de logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def formatar_cnj(numero: str) -> str:
    """
    Recebe um número de CNJ sem formatação e retorna no formato padrão:
    0000000-00.0000.0.00.0000
    """
    if not numero or not numero.isdigit() or len(numero) != 20:
        return numero  # Retorna como está se não tiver 20 dígitos
    return f"{numero[:7]}-{numero[7:9]}.{numero[9:13]}.{numero[13]}.{numero[14:16]}.{numero[16:]}"

@app.post("/upload-lista-precatorios", tags=["Popular DB"])
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
   

    # Valida extensão
    if not (file.filename.endswith(".csv") or file.filename.endswith(".xlsx")):
        logger.error("Arquivo inválido. Deve ser CSV ou XLSX")
        raise HTTPException(status_code=400, detail="Arquivo deve ser CSV ou XLSX")
    
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
 

    # Agenda exclusão do arquivo após envio
    background_tasks.add_task(delete_file, file_path)

    df = None
    if file.filename.endswith(".csv"):
        with open(file_path, "rb") as f:
            rawdata = f.read()
            result = chardet.detect(rawdata)
        encoding = result["encoding"] or "utf-8"
        logger.info(f"Encoding detectado: {encoding}")

        try:
            df = pd.read_csv(file_path, dtype=str, encoding=encoding, sep=None, engine='python')
            logger.info(f"CSV lido com sucesso. Total de linhas: {len(df)}")
        except Exception as e:
            logger.error(f"Erro ao ler CSV: {e}")
            raise HTTPException(status_code=500, detail=f"Erro ao ler CSV: {e}")
    
    elif file.filename.endswith(".xlsx"):
        try:
            df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
            logger.info(f"XLSX lido com sucesso. Total de linhas: {len(df)}")
        except Exception as e:
            logger.error(f"Erro ao ler XLSX: {e}")
            raise HTTPException(status_code=500, detail=f"Erro ao ler XLSX: {e}")

    df.columns = df.columns.str.strip().str.lower()
    if "numero" not in df.columns:
        logger.error("Coluna 'numero' não encontrada no arquivo")
        raise HTTPException(status_code=400, detail="O arquivo deve conter a coluna 'numero'")

    # Remove espaços, aplica formatação e remove duplicatas
    df['numero'] = df['numero'].str.strip().apply(formatar_cnj)
    df = df.drop_duplicates(subset=['numero'], keep='first')
    logger.info(f"Duplicatas internas removidas. Total de linhas únicas: {len(df)}")

    numeros_cnj_csv = df["numero"].dropna().tolist()
    logger.info(f"Total de CNJs após remover duplicatas internas: {len(numeros_cnj_csv)}")

    # Consulta CNJs existentes no banco
    async with AsyncSessionLocal() as session:
        existing_processos = await session.execute(
            select(Processo.numero_cnj).where(Processo.numero_cnj.in_(numeros_cnj_csv))
        )
        existing_cnjs = {p[0] for p in existing_processos}
    logger.info(f"CNJs já existentes no banco: {len(existing_cnjs)}")

    # Filtra CNJs que serão processados
    numeros_cnj_para_processar = [cnj for cnj in numeros_cnj_csv if cnj not in existing_cnjs]
    total_novos = len(numeros_cnj_para_processar)
    total_existentes = len(existing_cnjs)
    logger.info(f"CNJs a processar: {total_novos}")

    if not numeros_cnj_para_processar:
        aviso = f"Nenhum novo precatório para processar. {total_existentes} já existem no banco de dados."
        logger.info(aviso)
        return {"detail": aviso}

    # Processa em lotes
    batch_size = 200
    for start in range(0, total_novos, batch_size):
        batch = numeros_cnj_para_processar[start:start + batch_size]
        await processar_lote(batch)
        logger.info(f"Lote processado: {start} a {start + len(batch)}")

    aviso_final = (
        f"Arquivo processado com sucesso! "
        f"{total_novos} novos precatórios processados. "
        f"{total_existentes} precatórios já existentes foram ignorados."
    )
    logger.info(aviso_final)
    
    return {"detail": aviso_final}


def chunks(lst, n):
    """Divide uma lista em pedaços (chunks) de tamanho 'n'."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
def auto_ajustar_colunas(worksheet, dataframe):
    """Ajusta a largura das colunas de uma planilha Excel ao conteúdo do DataFrame."""
    for i, col in enumerate(dataframe.columns):
        column_letter = openpyxl.utils.get_column_letter(i + 1)
        max_length = 0
        
        # Pega o tamanho do cabeçalho da coluna
        header_length = len(str(col))
        max_length = max(max_length, header_length)
        
        # Pega o tamanho máximo do conteúdo da coluna
        for cell in worksheet[column_letter]:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except (TypeError, ValueError):
                pass
        
        # Define a largura da coluna com uma margem extra para melhor visualização
        adjusted_width = max_length + 2
        worksheet.column_dimensions[column_letter].width = adjusted_width

#--------------------------------------uploadlistacomplementar------

def chunks(lst, n):
    """Divide uma lista em pedaços (chunks) de tamanho 'n'."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

@app.post("/upload-dados-complementares-precatorios", tags=["Popular DB"])
async def upload_dados_precatorios(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Valida extensão
    if not (file.filename.endswith(".csv") or file.filename.endswith(".xlsx")):
        raise HTTPException(status_code=400, detail="Arquivo deve ser CSV ou XLSX")

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    background_tasks.add_task(delete_file, file_path)
    logger.info(f"Arquivo salvo temporariamente em: {file_path}")

    # Lê arquivo
    df = None
    if file.filename.endswith(".csv"):
        with open(file_path, "rb") as f:
            rawdata = f.read()
            result = chardet.detect(rawdata)
        encoding = result["encoding"] or "utf-8"
        try:
            df = pd.read_csv(file_path, dtype=str, encoding=encoding, sep=None, engine='python')
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro ao ler CSV: {e}")
    else:  # XLSX
        try:
            df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro ao ler XLSX: {e}")

    # Padroniza colunas
    df.columns = df.columns.str.strip().str.lower()
    if "numero" not in df.columns:
        raise HTTPException(status_code=400, detail="O arquivo deve conter a coluna 'numero'")

    # Normaliza CNJ e remove duplicatas
    df['numero'] = df['numero'].str.strip().apply(formatar_cnj)
    df = df.drop_duplicates(subset=['numero'], keep='first')

    # Consulta CNJs existentes no banco em lotes
    processos_map = {}
    cnj_list = df['numero'].tolist()
    chunk_size = 1000

    async with AsyncSessionLocal() as session:
        for cnj_chunk in chunks(cnj_list, chunk_size):
            existing_processos = await session.execute(
                select(Processo.numero_cnj, Processo.id).where(Processo.numero_cnj.in_(cnj_chunk))
            )
            processos_map.update({p.numero_cnj: p.id for p in existing_processos.all()})

        dados_precatorios_to_insert = []
        for _, row in df.iterrows():
            numero_cnj = row['numero']
            processo_id = processos_map.get(numero_cnj)
            if not processo_id:
                logger.warning(f"Processo com CNJ {numero_cnj} não encontrado. Ignorando linha.")
                continue
            
            # --- Início da correção para o Valor Deferido ---
            valor_deferido_str = row.get("valor_deferido")
            valor_deferido = None
            if valor_deferido_str:
                try:
                    # Remove o R$, os pontos e substitui a vírgula por ponto
                    cleaned_value = valor_deferido_str.replace("R$", "").replace(".", "").replace(",", ".").strip()
                    valor_deferido = float(cleaned_value)
                except (ValueError, TypeError):
                    logger.warning(f"Valor deferido inválido para CNJ {numero_cnj}: '{valor_deferido_str}'. Ignorando valor.")
            # --- Fim da correção para o Valor Deferido ---

            # Trata data base apenas como MM/YYYY
            data_base = None
            if row.get("data_base_calculo"):
                try:
                    data_base = datetime.strptime(row["data_base_calculo"], "%m/%Y").date().replace(day=1)
                except Exception:
                    logger.warning(f"Data base inválida para CNJ {numero_cnj}: {row['data_base_calculo']}")

            dados_precatorios_to_insert.append(
                DadosPrecatorio(
                    processo_id=processo_id,
                    tipo_regime=row.get("tipo_regime"),
                    ano_orcamentario=int(row["ano_orcamentario"]) if row.get("ano_orcamentario") else None,
                    natureza_precatorio=row.get("natureza_precatorio"),
                    valor_deferido=valor_deferido,
                    data_base_calculo=data_base,
                    data_expedicao=pd.to_datetime(row["data_expedicao"], errors="coerce").date() if row.get("data_expedicao") else None,
                )
            )

        if dados_precatorios_to_insert:
            session.add_all(dados_precatorios_to_insert)
            await session.commit()
            logger.info(f"{len(dados_precatorios_to_insert)} registros de DadosPrecatorio inseridos com sucesso.")

    return {"detail": f"Upload finalizado. {len(dados_precatorios_to_insert)} registros inseridos."}



def _digits(x, length=11) -> str:
    """Normaliza CPF/CNPJ como string, removendo caracteres não numéricos e completando zeros à esquerda."""
    if pd.isna(x): 
        return ""
    s = str(x).strip()
    s = re.sub(r"\D+", "", s)  # Remove tudo que não é dígito
    return s.zfill(length) 

@app.post("/download-lista-precatorios-4-buy-callix/{tribunal_sigla}", tags=["Relatórios"])
async def download_precatorios_zip(tribunal_sigla: str, background_tasks: BackgroundTasks):
    df_credores_list = []
    df_advogados_list = []

    relations_to_load = [
        joinedload(Processo.fontes).joinedload(Fonte.envolvidos).joinedload(Envolvido.advogados).joinedload(Advogado.oabs),
        joinedload(Processo.dados_precatorios),
        joinedload(Processo.fontes).joinedload(Fonte.capa).joinedload(Capa.valor_causa),
    ]

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Processo)
            .options(*relations_to_load)
            .where(Processo.unidade_origem_tribunal_sigla == tribunal_sigla)
        )
        processos = result.scalars().unique().all()

        if not processos:
            raise HTTPException(status_code=404, detail="Nenhum processo encontrado para o tribunal fornecido.")

        for p in processos:
            dados_precatorios = p.dados_precatorios
            tipo_regime = dados_precatorios.tipo_regime if dados_precatorios else None

            reu = next((e for f in p.fontes for e in f.envolvidos if e.polo == "PASSIVO"), None)
            credor = next((e for f in p.fontes for e in f.envolvidos if e.polo == "ATIVO" and not e.tipo_normalizado == "Advogado"), None)

            # Determina tipo do precatório
            tipo_precatorio = None
            if p.unidade_origem_tribunal_sigla and p.unidade_origem_tribunal_sigla.startswith("TRF"):
                tipo_precatorio = "Federal"
            elif reu and reu.nome and "estado" in reu.nome.lower():
                tipo_precatorio = "Estadual"
            elif reu and reu.nome and ("município" in reu.nome.lower() or "municipio de" in reu.nome.lower()):
                tipo_precatorio = "Municipal"

            # Valor da causa
            valor_causa = None
            for fonte in p.fontes:
                if fonte.capa and fonte.capa.valor_causa:
                    valor_causa = fonte.capa.valor_causa.valor
                    break

            # Formata como R$ xx.xxx,xx
            if valor_causa is not None:
                valor_causa_formatado = f"R$ {valor_causa:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                valor_causa_formatado = ""

            # Linha de credores
            row_credor = {
                "CNPJ / CPF do Credor": credor.cnpj if credor and credor.cnpj else credor.cpf if credor else None,
                "Nome do Credor": credor.nome if credor else None,
                "Tipo do Credor": "Pessoa Jurídica" if credor and credor.cnpj else "Pessoa Física" if credor and credor.cpf else None,
                "Nome do Réu": reu.nome if reu else None,
                "CNPJ do Réu": reu.cnpj if reu else None,
                "UF do Precatório": p.estado_origem,
                "Município do Precatório": p.unidade_origem_cidade if tipo_precatorio == "Municipal" else None,
                "Número dos Autos do Precatório": p.numero_cnj,
                "Tipo do Precatório": tipo_precatorio,
                "Tipo do Regime": tipo_regime,
                "Valor da Causa": valor_causa_formatado,
            }
            df_credores_list.append(row_credor)

            # Linhas de advogados
            for fonte in p.fontes:
                for envolvido in fonte.envolvidos:
                    if envolvido.polo == "ATIVO" and envolvido.advogados:
                        for advogado in envolvido.advogados:
                            if advogado and advogado.oabs:
                                for oab in advogado.oabs:
                                    row_advogado = {
                                        "Nome do Advogado": advogado.nome,
                                        "CPF do Advogado": advogado.cpf,
                                        "OAB": oab.numero,
                                        "Estado da OAB": oab.uf,
                                        "CNPJ / CPF do Credor": credor.cnpj if credor and credor.cnpj else credor.cpf if credor else None,
                                        "Nome do Credor": credor.nome if credor else None,
                                        "Tipo do Credor": "Pessoa Jurídica" if credor and credor.cnpj else "Pessoa Física" if credor and credor.cpf else None,
                                        "Nome do Réu": reu.nome if reu else None,
                                        "CNPJ do Réu": reu.cnpj if reu else None,
                                        "UF do Precatório": p.estado_origem,
                                        "Município do Precatório": p.unidade_origem_cidade if tipo_precatorio == "Municipal" else None,
                                        "Número dos Autos do Precatório": p.numero_cnj,
                                        "Tipo do Precatório": tipo_precatorio,
                                        "Tipo do Regime": tipo_regime,
                                        "Valor da Causa": valor_causa_formatado,
                                    }
                                    df_advogados_list.append(row_advogado)

    if not df_credores_list and not df_advogados_list:
        raise HTTPException(status_code=404, detail="Nenhum dado encontrado para o tribunal fornecido.")

    # Criar DataFrames
    df_credores = pd.DataFrame(df_credores_list)
    df_advogados = pd.DataFrame(df_advogados_list)

    # Garantir CPF/CNPJ como string, preenchendo zeros à esquerda
    if "CNPJ / CPF do Credor" in df_credores.columns:
        df_credores["CNPJ / CPF do Credor"] = df_credores["CNPJ / CPF do Credor"].apply(lambda x: _digits(x, 11) if len(str(x)) <= 11 else _digits(x, 14))
    if "CNPJ do Réu" in df_credores.columns:
        df_credores["CNPJ do Réu"] = df_credores["CNPJ do Réu"].apply(lambda x: _digits(x, 11) if len(str(x)) <= 11 else _digits(x, 14))
    if "CPF do Advogado" in df_advogados.columns:
        df_advogados["CPF do Advogado"] = df_advogados["CPF do Advogado"].apply(lambda x: _digits(x, 11))

    # Data de geração
    data_geracao = datetime.now().strftime("%Y%m%d%H%M%S")

    # Criar zip em memória
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            f"lista-callix-precatorios_credores_{tribunal_sigla}_{data_geracao}.csv",
            df_credores.to_csv(index=False, sep=';', encoding='utf-8', decimal=',', quotechar='"')
        )
        zip_file.writestr(
            f"lista-callix-precatorios_advogados_{tribunal_sigla}_{data_geracao}.csv",
            df_advogados.to_csv(index=False, sep=';', encoding='utf-8', decimal=',', quotechar='"')
        )

    zip_buffer.seek(0)
    zip_filename = f"lista-callix-precatorios_{tribunal_sigla}_{data_geracao}.zip"

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
    )




# --- Download CSV geral ---
@app.post("/download-csv/{tribunal_sigla}")
async def download_csv(tribunal_sigla: str, background_tasks: BackgroundTasks):
    df_list = []

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Processo)
            .options(
                joinedload(Processo.fontes).joinedload(Fonte.capa).joinedload(Capa.valor_causa),
                joinedload(Processo.fontes).joinedload(Fonte.envolvidos).joinedload(Envolvido.advogados).joinedload(Advogado.oabs),
                joinedload(Processo.processos_relacionados)
            )
            .where(Processo.unidade_origem_tribunal_sigla == tribunal_sigla)
        )
        processos = result.scalars().unique().all()
        if not processos:
            raise HTTPException(status_code=404, detail="Nenhum processo encontrado para o tribunal fornecido.")
      
        for p in processos:
            base_data = {
                "processo_numero_cnj": p.numero_cnj,
                "processo_titulo_polo_ativo": p.titulo_polo_ativo,
                "processo_titulo_polo_passivo": p.titulo_polo_passivo,
                "processo_ano_inicio": p.ano_inicio,
                "processo_data_inicio": p.data_inicio.isoformat() if p.data_inicio else None,
                "processo_estado_origem": p.estado_origem,
                "processo_unidade_origem_nome": p.unidade_origem_nome,
                "processo_unidade_origem_cidade": p.unidade_origem_cidade,
                "processo_unidade_origem_estado": p.unidade_origem_estado,
                "processo_unidade_origem_tribunal_sigla": p.unidade_origem_tribunal_sigla,
                "processo_data_ultima_movimentacao": p.data_ultima_movimentacao.isoformat() if p.data_ultima_movimentacao else None,
                "processo_quantidade_movimentacoes": p.quantidade_movimentacoes,
                "processo_fontes_tribunais_estao_arquivadas": p.fontes_tribunais_estao_arquivadas,
                "processo_data_ultima_verificacao": p.data_ultima_verificacao.isoformat() if p.data_ultima_verificacao else None,
                "processo_tempo_desde_ultima_verificacao": p.tempo_desde_ultima_verificacao,
                "processo_relacionado_numero": ", ".join([pr.numero for pr in p.processos_relacionados]),
            }

            has_related_data = False
            for fonte in p.fontes:
                capa = fonte.capa
                valor_causa = capa.valor_causa if capa else None
                
                if not fonte.envolvidos:
                    row = base_data.copy()
                    row.update({
                        "fonte_id": fonte.id, "fonte_descricao": fonte.descricao, "fonte_nome": fonte.nome, "fonte_sigla": fonte.sigla,
                        "fonte_tipo": fonte.tipo, "fonte_data_inicio": fonte.data_inicio.isoformat() if fonte.data_inicio else None,
                        "fonte_data_ultima_movimentacao": fonte.data_ultima_movimentacao.isoformat() if fonte.data_ultima_movimentacao else None,
                        "fonte_segredo_justica": fonte.segredo_justica, "fonte_arquivado": fonte.arquivado,
                        "fonte_status_predito": fonte.status_predito, "fonte_grau": fonte.grau, "fonte_grau_formatado": fonte.grau_formatado,
                        "fonte_fisico": fonte.fisico, "fonte_sistema": fonte.sistema, "fonte_quantidade_envolvidos": fonte.quantidade_envolvidos,
                        "fonte_url": fonte.url,
                        "capa_classe": capa.classe if capa else None, "capa_assunto": capa.assunto if capa else None,
                        "capa_orgao_julgador": capa.orgao_julgador if capa else None, "capa_situacao": capa.situacao if capa else None,
                        "capa_valor_causa": valor_causa.valor_formatado if valor_causa else None,
                        "envolvido_nome": None, "envolvido_tipo_normalizado": None, "envolvido_polo": None,
                        "envolvido_cpf": None, "envolvido_cnpj": None, "envolvido_tipo_pessoa": None,
                        "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None, "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
                    })
                    df_list.append(row)
                    has_related_data = True
                    continue

                for envolvido in fonte.envolvidos:
                    if not envolvido.advogados:
                        row = base_data.copy()
                        row.update({
                            "fonte_id": fonte.id, "fonte_descricao": fonte.descricao, "fonte_nome": fonte.nome, "fonte_sigla": fonte.sigla,
                            "fonte_tipo": fonte.tipo, "fonte_data_inicio": fonte.data_inicio.isoformat() if fonte.data_inicio else None,
                            "fonte_data_ultima_movimentacao": fonte.data_ultima_movimentacao.isoformat() if fonte.data_ultima_movimentacao else None,
                            "fonte_segredo_justica": fonte.segredo_justica, "fonte_arquivado": fonte.arquivado,
                            "fonte_status_predito": fonte.status_predito, "fonte_grau": fonte.grau, "fonte_grau_formatado": fonte.grau_formatado,
                            "fonte_fisico": fonte.fisico, "fonte_sistema": fonte.sistema, "fonte_quantidade_envolvidos": fonte.quantidade_envolvidos,
                            "fonte_url": fonte.url,
                            "capa_classe": capa.classe if capa else None, "capa_assunto": capa.assunto if capa else None,
                            "capa_orgao_julgador": capa.orgao_julgador if capa else None, "capa_situacao": capa.situacao if capa else None,
                            "capa_valor_causa": valor_causa.valor_formatado if valor_causa else None,
                            "envolvido_nome": envolvido.nome, "envolvido_tipo_normalizado": envolvido.tipo_normalizado, "envolvido_polo": envolvido.polo,
                            "envolvido_cpf": envolvido.cpf, "envolvido_cnpj": envolvido.cnpj, "envolvido_tipo_pessoa": envolvido.tipo_pessoa,
                            "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None, "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
                        })
                        df_list.append(row)
                        has_related_data = True
                        continue

                    for advogado in envolvido.advogados:
                        if not advogado.oabs:
                            row = base_data.copy()
                            row.update({
                                "fonte_id": fonte.id, "fonte_descricao": fonte.descricao, "fonte_nome": fonte.nome, "fonte_sigla": fonte.sigla,
                                "fonte_tipo": fonte.tipo, "fonte_data_inicio": fonte.data_inicio.isoformat() if fonte.data_inicio else None,
                                "fonte_data_ultima_movimentacao": fonte.data_ultima_movimentacao.isoformat() if fonte.data_ultima_movimentacao else None,
                                "fonte_segredo_justica": fonte.segredo_justica, "fonte_arquivado": fonte.arquivado,
                                "fonte_status_predito": fonte.status_predito, "fonte_grau": fonte.grau, "fonte_grau_formatado": fonte.grau_formatado,
                                "fonte_fisico": fonte.fisico, "fonte_sistema": fonte.sistema, "fonte_quantidade_envolvidos": fonte.quantidade_envolvidos,
                                "fonte_url": fonte.url,
                                "capa_classe": capa.classe if capa else None, "capa_assunto": capa.assunto if capa else None,
                                "capa_orgao_julgador": capa.orgao_julgador if capa else None, "capa_situacao": capa.situacao if capa else None,
                                "capa_valor_causa": valor_causa.valor_formatado if valor_causa else None,
                                "envolvido_nome": envolvido.nome, "envolvido_tipo_normalizado": envolvido.tipo_normalizado, "envolvido_polo": envolvido.polo,
                                "envolvido_cpf": envolvido.cpf, "envolvido_cnpj": envolvido.cnpj, "envolvido_tipo_pessoa": envolvido.tipo_pessoa,
                                "advogado_nome": advogado.nome, "advogado_tipo": advogado.tipo_normalizado, "advogado_oab": None,
                                "advogado_cpf": advogado.cpf, "advogado_cnpj": advogado.cnpj, "advogado_tipo_pessoa": advogado.tipo_pessoa,
                            })
                            df_list.append(row)
                            has_related_data = True
                            continue

                        for oab in advogado.oabs:
                            row = base_data.copy()
                            row.update({
                                "fonte_id": fonte.id, "fonte_descricao": fonte.descricao, "fonte_nome": fonte.nome, "fonte_sigla": fonte.sigla,
                                "fonte_tipo": fonte.tipo, "fonte_data_inicio": fonte.data_inicio.isoformat() if fonte.data_inicio else None,
                                "fonte_data_ultima_movimentacao": fonte.data_ultima_movimentacao.isoformat() if fonte.data_ultima_movimentacao else None,
                                "fonte_segredo_justica": fonte.segredo_justica, "fonte_arquivado": fonte.arquivado,
                                "fonte_status_predito": fonte.status_predito, "fonte_grau": fonte.grau, "fonte_grau_formatado": fonte.grau_formatado,
                                "fonte_fisico": fonte.fisico, "fonte_sistema": fonte.sistema, "fonte_quantidade_envolvidos": fonte.quantidade_envolvidos,
                                "fonte_url": fonte.url,
                                "capa_classe": capa.classe if capa else None, "capa_assunto": capa.assunto if capa else None,
                                "capa_orgao_julgador": capa.orgao_julgador if capa else None, "capa_situacao": capa.situacao if capa else None,
                                "capa_valor_causa": valor_causa.valor_formatado if valor_causa else None,
                                "envolvido_nome": envolvido.nome, "envolvido_tipo_normalizado": envolvido.tipo_normalizado, "envolvido_polo": envolvido.polo,
                                "envolvido_cpf": envolvido.cpf, "envolvido_cnpj": envolvido.cnpj, "envolvido_tipo_pessoa": envolvido.tipo_pessoa,
                                "advogado_nome": advogado.nome, "advogado_tipo": advogado.tipo_normalizado, "advogado_oab": f"{oab.numero}/{oab.uf}",
                                "advogado_cpf": advogado.cpf, "advogado_cnpj": advogado.cnpj, "advogado_tipo_pessoa": advogado.tipo_pessoa,
                            })
                            df_list.append(row)
                            has_related_data = True

            if not has_related_data:
                row = base_data.copy()
                row.update({
                    "fonte_id": None, "fonte_descricao": None, "fonte_nome": None, "fonte_sigla": None, "fonte_tipo": None, "fonte_data_inicio": None,
                    "fonte_data_ultima_movimentacao": None, "fonte_segredo_justica": None, "fonte_arquivado": None, "fonte_status_predito": None,
                    "fonte_grau": None, "fonte_grau_formatado": None, "fonte_fisico": None, "fonte_sistema": None, "fonte_quantidade_envolvidos": None,
                    "fonte_url": None, "capa_classe": None, "capa_assunto": None, "capa_orgao_julgador": None, "capa_situacao": None,
                    "capa_valor_causa": None, "envolvido_nome": None, "envolvido_tipo_normalizado": None, "envolvido_polo": None,
                    "envolvido_cpf": None, "envolvido_cnpj": None, "envolvido_tipo_pessoa": None, "advogado_nome": None, "advogado_tipo": None,
                    "advogado_oab": None, "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
                })
                df_list.append(row)

    if not df_list:
        raise HTTPException(status_code=404, detail="Nenhum processo encontrado para o tribunal fornecido.")

    df_export = pd.DataFrame(df_list)

    # Convertendo as colunas de CPF e CNPJ para string, preenchendo nulos e adicionando o apóstrofo
    colunas_para_formatar = ["envolvido_cpf", "envolvido_cnpj", "advogado_cpf", "advogado_cnpj"]
    for col in colunas_para_formatar:
        if col in df_export.columns:
            # Preenche nulos com string vazia e adiciona o apóstrofo para forçar formato de texto
            df_export[col] = df_export[col].fillna('').astype(str).apply(lambda x: f"'{x}")

    colunas_ordenadas = [
        "processo_numero_cnj", "envolvido_nome", "envolvido_tipo_normalizado", "envolvido_polo", "envolvido_cpf", "envolvido_cnpj", "envolvido_tipo_pessoa",
        "advogado_nome", "advogado_tipo", "advogado_oab", "advogado_cpf", "advogado_cnpj", "advogado_tipo_pessoa",
        "processo_titulo_polo_ativo", "processo_titulo_polo_passivo", "processo_ano_inicio", "processo_data_inicio", "processo_estado_origem",
        "processo_unidade_origem_nome", "processo_unidade_origem_cidade", "processo_unidade_origem_estado", "processo_unidade_origem_tribunal_sigla",
        "processo_data_ultima_movimentacao", "processo_quantidade_movimentacoes", "processo_fontes_tribunais_estao_arquivadas", "processo_data_ultima_verificacao",
        "processo_tempo_desde_ultima_verificacao", "processo_relacionado_numero",
        "fonte_id", "fonte_descricao", "fonte_nome", "fonte_sigla", "fonte_tipo", "fonte_data_inicio", "fonte_data_ultima_movimentacao",
        "fonte_segredo_justica", "fonte_arquivado", "fonte_status_predito", "fonte_grau", "fonte_grau_formatado",
        "fonte_fisico", "fonte_sistema", "fonte_quantidade_envolvidos", "fonte_url",
        "capa_classe", "capa_assunto", "capa_orgao_julgador", "capa_situacao", "capa_valor_causa",
    ]
    df_export = df_export.reindex(columns=colunas_ordenadas, fill_value=None)
    
    file_name = f"relatorio_{tribunal_sigla}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    file_path = os.path.join(UPLOAD_DIR, file_name)
    
    df_export.to_csv(file_path, index=False)

    background_tasks.add_task(delete_file, file_path)
    
    return FileResponse(file_path, media_type="text/csv", filename=file_name)

# Diretório para salvar os arquivos CSV
UPLOAD_DIR = "/tmp"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)


# Função para excluir o arquivo CSV após o download
def delete_file(file_path: str):
    os.remove(file_path)


# --- Download XLSX de Precatórios ---
@app.post("/download-lista-precatorios/{tribunal_sigla}", tags=["Relatórios"])
async def download_precatorios_csv(tribunal_sigla: str, background_tasks: BackgroundTasks):
    """
    Gera e retorna um arquivo Excel (.xlsx) com dados de precatórios para um tribunal específico.

    O endpoint busca todos os processos de um tribunal e extrai informações
    de precatórios dos envolvidos no polo ativo (credor) e passivo (réu).
    As informações são organizadas em duas abas separadas na mesma planilha.
    """
    df_credores_list = []
    df_advogados_list = []
    
    # A consulta agora busca os dados diretamente da tabela DadosPrecatorio
    relations_to_load = [
        joinedload(Processo.fontes).joinedload(Fonte.envolvidos).joinedload(Envolvido.advogados).joinedload(Advogado.oabs),
        joinedload(Processo.dados_precatorios)
    ]

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Processo)
            .options(*relations_to_load)
            .where(Processo.unidade_origem_tribunal_sigla == tribunal_sigla)
        )
        processos = result.scalars().unique().all()

        if not processos:
            raise HTTPException(status_code=404, detail="Nenhum processo encontrado para o tribunal fornecido.")

        for p in processos:
            # Novo: Acessa os dados diretamente da nova relação `dados_precatorios`
            dados_precatorios = p.dados_precatorios
            
            tipo_regime = dados_precatorios.tipo_regime if dados_precatorios else None
            ano_orcamentario = dados_precatorios.ano_orcamentario if dados_precatorios else None
            natureza_precatorio = dados_precatorios.natureza_precatorio if dados_precatorios else None
            valor_deferido = dados_precatorios.valor_deferido if dados_precatorios else None
            data_base_calculo = dados_precatorios.data_base_calculo if dados_precatorios else None
            data_expedicao = dados_precatorios.data_expedicao if dados_precatorios else None

            # Encontra o réu e o credor (lógica mantida)
            reu = next((e for f in p.fontes for e in f.envolvidos if e.polo == "PASSIVO"), None)
            credor = next((e for f in p.fontes for e in f.envolvidos if e.polo == "ATIVO" and not e.tipo_normalizado == "Advogado"), None)
            
            # Lógica de classificação do tipo de precatório (lógica mantida)
            tipo_precatorio = None
            if p.unidade_origem_tribunal_sigla and p.unidade_origem_tribunal_sigla.startswith("TRF"):
                tipo_precatorio = "Federal"
            elif reu and reu.nome and "estado" in reu.nome.lower():
                tipo_precatorio = "Estadual"
            elif reu and reu.nome and "município" in reu.nome.lower():
                tipo_precatorio = "Municipal"

            row_credor = {
                "CNPJ / CPF do Credor": credor.cnpj if credor and credor.cnpj else credor.cpf if credor else None,
                "Nome do Credor": credor.nome if credor else None,
                "Tipo do Credor": "Pessoa Jurídica" if credor and credor.cnpj else "Pessoa Física" if credor and credor.cpf else None,
                "Nome do Réu": reu.nome if reu else None,
                "CNPJ do Réu": reu.cnpj if reu else None,
                "UF do Precatório": p.estado_origem,
                "Município do Precatório": p.unidade_origem_cidade,
                "Número dos Autos do Precatório": p.numero_cnj,
                "Tipo do Precatório": tipo_precatorio,
                "Tipo do Regime": tipo_regime,
                "Ano orçamentário": ano_orcamentario,
                "Natureza do Precatório": natureza_precatorio,
                "Valor Deferido": valor_deferido,
                "Data base do cálculo homologado": data_base_calculo,
                "Data de expedição do Precatório": data_expedicao,
            }
            df_credores_list.append(row_credor)

            for fonte in p.fontes:
                for envolvido in fonte.envolvidos:
                    if envolvido.polo == "ATIVO" and envolvido.advogados:
                        for advogado in envolvido.advogados:
                            if advogado and advogado.oabs:
                                for oab in advogado.oabs:
                                    row_advogado = {
                                        "Nome do Advogado": advogado.nome,
                                        "CPF do Advogado": advogado.cpf,
                                        "OAB": oab.numero,
                                        "Estado da OAB": oab.uf,
                                        "Nome do Réu": reu.nome if reu else None,
                                        "CNPJ do Réu": reu.cnpj if reu else None,
                                        "UF do Precatório": p.estado_origem,
                                        "Município do Precatório": p.unidade_origem_cidade,
                                        "Número dos Autos do Precatório": p.numero_cnj,
                                        "Tipo do Precatório": tipo_precatorio,
                                        "Tipo do Regime": tipo_regime,
                                        "Ano orçamentário": ano_orcamentario,
                                        "Natureza do Precatório": natureza_precatorio,
                                        "Valor Deferido": valor_deferido,
                                        "Data base do cálculo homologado": data_base_calculo,
                                        "Data de expedição do Precatório": data_expedicao,
                                    }
                                    df_advogados_list.append(row_advogado)

    if not df_credores_list and not df_advogados_list:
        raise HTTPException(status_code=404, detail="Nenhum dado de precatório ou advogado encontrado para o tribunal fornecido.")

    df_credores = pd.DataFrame(df_credores_list)
    df_advogados = pd.DataFrame(df_advogados_list)

    colunas_formatar_credores = ["CNPJ / CPF do Credor", "CNPJ do Réu"]
    for col in colunas_formatar_credores:
        if col in df_credores.columns:
            df_credores[col] = df_credores[col].fillna('').astype(str).apply(lambda x: f"'{x}")

    colunas_formatar_advogados = ["CPF do Advogado", "CNPJ do Réu"]
    for col in colunas_formatar_advogados:
        if col in df_advogados.columns:
            df_advogados[col] = df_advogados[col].fillna('').astype(str).apply(lambda x: f"'{x}")

    file_name = f"relatorio_precatorios_{tribunal_sigla}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    file_path = os.path.join(UPLOAD_DIR, file_name)

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df_credores.to_excel(writer, sheet_name='Credores', index=False)
        df_advogados.to_excel(writer, sheet_name='Advogados', index=False)

    background_tasks.add_task(delete_file, file_path)

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_name,
    )





# --- Endpoint de remoção de duplicatas revisado ---

@app.post("/remover-duplicatas/", tags=["Ferramentas"])
async def remover_duplicatas(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    filename = file.filename
    if not filename.endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(status_code=400, detail="Formato de arquivo inválido. Envie .xlsx, .xls ou .csv.")

    conteudo_arquivo = await file.read()
    buffer = io.BytesIO(conteudo_arquivo)

    # Lê arquivo original
    if filename.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(buffer)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        novo_filename = f"arquivo_sem_duplicatas_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    else:
        detected_encoding = chardet.detect(conteudo_arquivo)['encoding']
        buffer.seek(0)
        df = pd.read_csv(buffer, dtype=str, sep=None, engine='python', encoding=detected_encoding)
        media_type = "text/csv"
        novo_filename = f"arquivo_sem_duplicatas_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

    # Remove duplicatas
    if "numero" not in df.columns:
        raise HTTPException(status_code=400, detail="Coluna 'numero' não encontrada.")
    df_sem_duplicatas = df.drop_duplicates(subset=['numero'], keep='first')

    # Salva em arquivo temporário
    file_path = os.path.join(UPLOAD_DIR, novo_filename)
    if novo_filename.endswith(".csv"):
        df_sem_duplicatas.to_csv(file_path, index=False, encoding='utf-8')
    else:
        df_sem_duplicatas.to_excel(file_path, index=False)

    # Agenda a exclusão após o envio
    background_tasks.add_task(delete_file, file_path)

    return FileResponse(file_path, media_type=media_type, filename=novo_filename)


# ---------------------------wesley-------------------------------
def _only_digits(value):
    """
    Remove todos os caracteres que não sejam dígitos de uma string.
    """
    if isinstance(value, str):
        return re.sub(r'\D', '', value)
    return value



# se não existir no teu app, define um default:
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "./exports")

@app.post("/download-requerentes-advogados-xlsx/{tribunal_sigla}", tags=["Relatórios"])
async def download_requerentes_advogados_xlsx(tribunal_sigla: str):
    """
    Gera e retorna um XLSX com duas abas:
      • Requerentes: dedup POR PROCESSO (chave = (processo_numero_cnj, CPF)).
                     Sem CPF => não deduplica.
      • Advogados  : dedup POR PROCESSO (chave = (processo_numero_cnj, CPF)).
                     Sem CPF => não deduplica.

    Mantém o processo na planilha mesmo se não houver Autor/Requerente/Advogado.
    """

    df_list = []

    # ===== Carrega processos com tudo que precisamos =====
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Processo)
            .options(
                joinedload(Processo.fontes)
                    .joinedload(Fonte.capa)
                    .joinedload(Capa.valor_causa),
                joinedload(Processo.fontes)
                    .joinedload(Fonte.envolvidos)
                    .joinedload(Envolvido.advogados)
                    .joinedload(Advogado.oabs),
                joinedload(Processo.processos_relacionados),
            )
            .where(Processo.unidade_origem_tribunal_sigla == tribunal_sigla)
        )
        processos = result.scalars().unique().all()

    if not processos:
        raise HTTPException(status_code=404, detail="Nenhum processo encontrado para o tribunal fornecido.")

    # ===== Monta linhas =====
    for p in processos:
        base = {
            "processo_numero_cnj": p.numero_cnj,
            "processo_ano_inicio": p.ano_inicio,
            "processo_data_inicio": p.data_inicio.isoformat() if p.data_inicio else None,
            "processo_estado_origem": p.estado_origem,
            "processo_unidade_origem_nome": p.unidade_origem_nome,
            "processo_unidade_origem_cidade": p.unidade_origem_cidade,
            "processo_unidade_origem_estado": p.unidade_origem_estado,
            "processo_unidade_origem_tribunal_sigla": p.unidade_origem_tribunal_sigla,
            "processo_data_ultima_movimentacao": p.data_ultima_movimentacao.isoformat() if p.data_ultima_movimentacao else None,
            "processo_quantidade_movimentacoes": p.quantidade_movimentacoes,
            "processo_fontes_tribunais_estao_arquivadas": p.fontes_tribunais_estao_arquivadas,
            "processo_data_ultima_verificacao": p.data_ultima_verificacao.isoformat() if p.data_ultima_verificacao else None,
            "processo_tempo_desde_ultima_verificacao": p.tempo_desde_ultima_verificacao,
            "processo_relacionado_numero": ", ".join([pr.numero for pr in p.processos_relacionados]),
        }

        added_any_for_process = False  # garante pelo menos 1 linha por processo

        fontes = p.fontes or []
        if not fontes:
            # Sem fontes: garante linha base
            df_list.append({
                **base,
                "fonte_sigla": None, "fonte_data_inicio": None, "fonte_sistema": None, "fonte_quantidade_envolvidos": None,
                "capa_classe": None, "capa_assunto": None, "capa_valor_causa": None,
                "envolvido_nome": None, "envolvido_tipo_normalizado": None,
                "envolvido_cpf": None, "envolvido_cnpj": None, "envolvido_tipo_pessoa": None,
                "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None,
                "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
            })
            continue

        for fonte in fontes:
            capa = fonte.capa
            row_common = {
                **base,
                "fonte_sigla": fonte.sigla,
                "fonte_data_inicio": fonte.data_inicio.isoformat() if fonte.data_inicio else None,
                "fonte_sistema": fonte.sistema,
                "fonte_quantidade_envolvidos": fonte.quantidade_envolvidos,
                "capa_classe": getattr(capa, "classe", None) if capa else None,
                "capa_assunto": getattr(capa, "assunto", None) if capa else None,
                "capa_valor_causa": (capa.valor_causa.valor_formatado if (capa and capa.valor_causa) else None),
            }

            envolvidos = fonte.envolvidos or []
            if not envolvidos:
                # Sem envolvidos: ainda assim registra linha base da fonte
                df_list.append({
                    **row_common,
                    "envolvido_nome": None, "envolvido_tipo_normalizado": None,
                    "envolvido_cpf": None, "envolvido_cnpj": None, "envolvido_tipo_pessoa": None,
                    "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None,
                    "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
                })
                added_any_for_process = True
                continue

            for envolvido in envolvidos:
                # Aceita Requerente e Autor; demais tipos também manterão a linha base
                if envolvido.tipo_normalizado not in ("Requerente", "Autor"):
                    df_list.append({
                        **row_common,
                        "envolvido_nome": None, "envolvido_tipo_normalizado": None,
                        "envolvido_cpf": None, "envolvido_cnpj": None, "envolvido_tipo_pessoa": None,
                        "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None,
                        "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
                    })
                    added_any_for_process = True
                    continue

                # Sem advogados → registra mesmo assim
                if not (envolvido.advogados or []):
                    df_list.append({
                        **row_common,
                        "envolvido_nome": envolvido.nome,
                        "envolvido_tipo_normalizado": envolvido.tipo_normalizado,
                        "envolvido_cpf": envolvido.cpf,
                        "envolvido_cnpj": envolvido.cnpj,
                        "envolvido_tipo_pessoa": envolvido.tipo_pessoa,
                        "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None,
                        "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
                    })
                    added_any_for_process = True
                    continue

                # Com advogados
                for advogado in envolvido.advogados:
                    oabs = advogado.oabs or [None]
                    for oab in oabs:
                        df_list.append({
                            **row_common,
                            "envolvido_nome": envolvido.nome,
                            "envolvido_tipo_normalizado": envolvido.tipo_normalizado,
                            "envolvido_cpf": envolvido.cpf,
                            "envolvido_cnpj": envolvido.cnpj,
                            "envolvido_tipo_pessoa": envolvido.tipo_pessoa,
                            "advogado_nome": advogado.nome,
                            "advogado_tipo": advogado.tipo_normalizado,
                            "advogado_oab": (f"{getattr(oab, 'numero', None)}/{getattr(oab, 'uf', None)}"
                                             if oab and getattr(oab, "numero", None) and getattr(oab, "uf", None) else None),
                            "advogado_cpf": advogado.cpf,
                            "advogado_cnpj": advogado.cnpj,
                            "advogado_tipo_pessoa": advogado.tipo_pessoa,
                        })
                        added_any_for_process = True

        if not added_any_for_process:
            # redundância de segurança
            df_list.append({
                **base,
                "fonte_sigla": None, "fonte_data_inicio": None, "fonte_sistema": None, "fonte_quantidade_envolvidos": None,
                "capa_classe": None, "capa_assunto": None, "capa_valor_causa": None,
                "envolvido_nome": None, "envolvido_tipo_normalizado": None,
                "envolvido_cpf": None, "envolvido_cnpj": None, "envolvido_tipo_pessoa": None,
                "advogado_nome": None, "advogado_tipo": None, "advogado_oab": None,
                "advogado_cpf": None, "advogado_cnpj": None, "advogado_tipo_pessoa": None,
            })

    if not df_list:
        raise HTTPException(status_code=404, detail="Nada a exportar.")

    df = pd.DataFrame(df_list)

    # =========================
    # Requerentes — DEDUP (CNJ+CPF). SEM CPF = NÃO DEDUP
    # =========================
    req_cols = [
        "processo_numero_cnj", "envolvido_nome", "envolvido_tipo_normalizado",
        "envolvido_cpf", "envolvido_cnpj", "envolvido_tipo_pessoa",
        "processo_ano_inicio", "processo_data_inicio", "processo_estado_origem",
        "processo_unidade_origem_nome", "processo_unidade_origem_cidade", "processo_unidade_origem_estado",
        "processo_unidade_origem_tribunal_sigla", "processo_data_ultima_movimentacao",
        "processo_quantidade_movimentacoes", "processo_fontes_tribunais_estao_arquivadas",
        "processo_data_ultima_verificacao", "processo_tempo_desde_ultima_verificacao",
        "processo_relacionado_numero",
        "fonte_sigla", "fonte_data_inicio", "fonte_sistema", "fonte_quantidade_envolvidos",
        "capa_classe", "capa_assunto", "capa_valor_causa",
    ]
    req = df.reindex(columns=req_cols).copy()

    req["cpf_clean"] = (
        req["envolvido_cpf"]
        .astype(str)
        .str.replace(r"\D+", "", regex=True)
    )

    req_com_cpf = req[req["cpf_clean"].str.len() > 0]
    req_sem_cpf = req[req["cpf_clean"].str.len() == 0]

    req_com_cpf = req_com_cpf.drop_duplicates(
        subset=["processo_numero_cnj", "cpf_clean"],
        keep="first"
    )

    req_final = pd.concat([req_com_cpf, req_sem_cpf], ignore_index=True)
    req_final = req_final.drop(columns=["cpf_clean"])

    # =========================
    # Advogados — DEDUP (CNJ+CPF). SEM CPF = NÃO DEDUP
    # =========================
    adv_cols_keep = [
        "advogado_nome", "advogado_tipo", "advogado_oab", "advogado_cpf", "advogado_cnpj", "advogado_tipo_pessoa",
        "processo_numero_cnj",
        "processo_ano_inicio", "processo_data_inicio", "processo_estado_origem",
        "processo_unidade_origem_nome", "processo_unidade_origem_cidade", "processo_unidade_origem_estado",
        "processo_unidade_origem_tribunal_sigla", "processo_data_ultima_movimentacao",
        "processo_quantidade_movimentacoes", "processo_fontes_tribunais_estao_arquivadas",
        "processo_data_ultima_verificacao", "processo_tempo_desde_ultima_verificacao",
        "processo_relacionado_numero",
        "fonte_sigla", "fonte_data_inicio", "fonte_sistema", "fonte_quantidade_envolvidos",
        "capa_classe", "capa_assunto", "capa_valor_causa",
    ]
    adv = df.reindex(columns=adv_cols_keep).copy()

    adv["cpf_clean"] = (
        adv["advogado_cpf"]
        .astype(str)
        .str.replace(r"\D+", "", regex=True)
    )

    adv_com_cpf = adv[adv["cpf_clean"].str.len() > 0]
    adv_sem_cpf = adv[adv["cpf_clean"].str.len() == 0]

    adv_com_cpf = adv_com_cpf.drop_duplicates(
        subset=["processo_numero_cnj", "cpf_clean"],
        keep="first"
    )

    adv_final = pd.concat([adv_com_cpf, adv_sem_cpf], ignore_index=True)
    adv_final = adv_final.drop(columns=["cpf_clean"])

    # =========================
    # Escreve XLSX (duas abas)
    # =========================
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    file_name = f"requerentes_advogados_{tribunal_sigla}_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    file_path = os.path.join(UPLOAD_DIR, file_name)

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        req_final.to_excel(writer, index=False, sheet_name="Requerentes")
        adv_final.to_excel(writer, index=False, sheet_name="Advogados")

        wb = writer.book
        fmt_text = wb.add_format({"num_format": "@", "text_wrap": False})

        # Requerentes
        ws_req = writer.sheets["Requerentes"]
        ws_req.set_column(0, len(req_final.columns) - 1, 18)
        # formata como texto se as colunas existirem
        col_map_req = {c: i for i, c in enumerate(req_final.columns)}
        if "envolvido_cpf" in col_map_req:
            ws_req.set_column(col_map_req["envolvido_cpf"], col_map_req["envolvido_cpf"], 18, fmt_text)
        if "envolvido_cnpj" in col_map_req:
            ws_req.set_column(col_map_req["envolvido_cnpj"], col_map_req["envolvido_cnpj"], 18, fmt_text)
        ws_req.autofilter(0, 0, max(len(req_final), 1), len(req_final.columns) - 1)
        ws_req.freeze_panes(1, 0)

        # Advogados
        ws_adv = writer.sheets["Advogados"]
        ws_adv.set_column(0, len(adv_final.columns) - 1, 18)
        col_map_adv = {c: i for i, c in enumerate(adv_final.columns)}
        if "advogado_cpf" in col_map_adv:
            ws_adv.set_column(col_map_adv["advogado_cpf"], col_map_adv["advogado_cpf"], 18, fmt_text)
        if "advogado_cnpj" in col_map_adv:
            ws_adv.set_column(col_map_adv["advogado_cnpj"], col_map_adv["advogado_cnpj"], 18, fmt_text)
        ws_adv.autofilter(0, 0, max(len(adv_final), 1), len(adv_final.columns) - 1)
        ws_adv.freeze_panes(1, 0)

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_name,
    )
