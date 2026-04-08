"""
Fix definitivo: painel.html
Salva token em localStorage no doLogin.
"""
path = "/opt/automacoes/cliquedf/comercial/static/painel.html"
c = open(path).read()

# Fix doLogin — salvar em localStorage
old = "    token = d.token; usuario = d.usuario;\n    _sSet('hc_p_token', token);\n    _sSet('hc_p_usuario', JSON.stringify(usuario));\n    mostrarApp();"
new = "    token = d.token; usuario = d.usuario;\n    localStorage.setItem('hc_p_token', token);\n    localStorage.setItem('hc_p_usuario', JSON.stringify(usuario));\n    _sSet('hc_p_token', token);\n    _sSet('hc_p_usuario', JSON.stringify(usuario));\n    mostrarApp();"
assert old in c, "ERRO: doLogin não encontrado"
c = c.replace(old, new)
print("[OK] doLogin salva em localStorage")

open(path, "w").write(c)
print("Arquivo salvo.")
