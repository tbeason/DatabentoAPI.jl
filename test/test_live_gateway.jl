using Test
using DatabentoAPI: gateway_for_dataset, DEFAULT_LIVE_PORT

@testset "live gateway" begin
    @test gateway_for_dataset("GLBX.MDP3") == "glbx-mdp3.lsg.databento.com"
    @test gateway_for_dataset("XNAS.ITCH") == "xnas-itch.lsg.databento.com"
    @test gateway_for_dataset("OPRA.PILLAR") == "opra-pillar.lsg.databento.com"
    @test DEFAULT_LIVE_PORT == 13000
end
