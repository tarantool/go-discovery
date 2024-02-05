local auth_type = os.getenv('TEST_TNT_AUTH_TYPE')
if auth_type == 'auto' then
    auth_type = nil
end

box.cfg{
    auth_type = auth_type,
    work_dir = os.getenv('TEST_TNT_WORK_DIR'),
}

box.once('schema', function()
    box.schema.user.create('testuser', {password = 'testpass'})
    box.schema.user.grant('testuser', 'replication')

    box.schema.user.grant('testuser', 'create,read,write,drop,alter', 'space')
    box.schema.user.grant('testuser', 'execute', 'universe')
end)

-- Set listen only when every other thing is configured.
box.cfg{
    auth_type = auth_type,
    listen = os.getenv('TEST_TNT_LISTEN'),
}

require('console').start()
