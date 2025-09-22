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
from app.models import Processo, ProcessoRelacionado, Fonte, Envolvido, Advogado, OAB, ValorCausa, Capa
from datetime import datetime
import csv
import io
import logging
app = FastAPI(title="API de Processamento de Precatórios By MARIO B.")

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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/upload-csv")
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    logger.info(f"Iniciando upload do arquivo: {file.filename}")

    if not file.filename.endswith(".csv"):
        logger.error("Arquivo inválido. Deve ser CSV")
        raise HTTPException(status_code=400, detail="Arquivo deve ser CSV")
    
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())
    logger.info(f"Arquivo salvo temporariamente em: {file_path}")

    # Agenda exclusão do arquivo após envio
    background_tasks.add_task(delete_file, file_path)

    # Detecta encoding
    with open(file_path, "rb") as f:
        rawdata = f.read()
        result = chardet.detect(rawdata)
    encoding = result["encoding"] or "utf-8"
    logger.info(f"Encoding detectado: {encoding}")

    # Lê CSV
    try:
        df = pd.read_csv(file_path, dtype=str, encoding=encoding, sep=None, engine='python')
        logger.info(f"CSV lido com sucesso. Total de linhas: {len(df)}")
    except Exception as e:
        logger.error(f"Erro ao ler CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao ler CSV: {e}")
    
    if "numero" not in df.columns:
        logger.error("Coluna 'numero' não encontrada no CSV")
        raise HTTPException(status_code=400, detail="CSV deve conter a coluna 'numero'")
     # Remove espaços em branco antes e depois de cada CNJ
    df['numero'] = df['numero'].str.strip()
    logger.info("Espaços em branco nos CNJs removidos")

    # --- Remove duplicatas exatas dentro do CSV ---
    df = df.drop_duplicates(subset=['numero'], keep='first')
    logger.info(f"Duplicatas internas removidas. Total de linhas únicas: {len(df)}")

    numeros_cnj_csv = df["numero"].dropna().tolist()
    logger.info(f"Total de CNJs após remover duplicatas internas: {len(numeros_cnj_csv)}")

    # Verifica CNJs já existentes no banco
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

# --- Endpoint de remoção de duplicatas revisado ---

@app.post("/remover-duplicatas/")
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


# --- Novo Endpoint para baixar apenas Requerentes ---
@app.post("/download-requerentes-csv/{tribunal_sigla}")
async def download_requerentes_csv(tribunal_sigla: str, background_tasks: BackgroundTasks):
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

        # Itera sobre os processos e coleta os dados
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

            for fonte in p.fontes:
                capa = fonte.capa
                valor_causa = capa.valor_causa if capa else None
                
                for envolvido in fonte.envolvidos:
                    # Filtra para incluir apenas envolvidos do tipo 'Requerente'
                    if envolvido.tipo_normalizado != "Requerente":
                        continue
                    
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

    if not df_list:
        raise HTTPException(status_code=404, detail="Nenhum processo encontrado com envolvidos requerentes para o tribunal fornecido.")

    df_export = pd.DataFrame(df_list)

    # Convertendo as colunas de CPF e CNPJ para string, preenchendo nulos e adicionando o apóstrofo
    colunas_para_formatar = ["envolvido_cpf", "envolvido_cnpj", "advogado_cpf", "advogado_cnpj"]
    for col in colunas_para_formatar:
        if col in df_export.columns:
            # Preenche nulos com string vazia e adiciona o apóstrofo para forçar formato de texto
            df_export[col] = df_export[col].fillna('').astype(str).apply(lambda x: f"'{x}")

    # Definindo a ordem das colunas para exportação
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
    
    file_name = f"relatorio_requerentes_{tribunal_sigla}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    file_path = os.path.join(UPLOAD_DIR, file_name)
    
    df_export.to_csv(file_path, index=False)
    
    background_tasks.add_task(delete_file, file_path)
    
    return FileResponse(file_path, media_type="text/csv", filename=file_name)
