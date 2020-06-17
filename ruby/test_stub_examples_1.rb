require File.expand_path(File.dirname(__FILE__)) + '/../../test_helper'

class Profile::ScheduleFormModelTest < ActiveSupport::TestCase

  def setup
    @professional_id = 34384
    @service_name = "appointment_scheduling"
    @config_name = "scheduling_url"
    @config_value = "https://www.example.com"
    @scheduling_service = Array(OpenStruct.new(professional_id: @professional_id,
                                               service_name: @service_name,
                                               config_name: @config_name,
                                               config_value: @config_value))
  end

  def test_find
    Solicitor::Client::ProfessionalExternalServices.stubs(:where).returns(@scheduling_service)
    model = Profile::ScheduleForm.find(@professional_id)
    assert_equal(@professional_id, model.professional_id)
    assert_equal(@config_value, model.scheduling_url)
  end

  def test_find_missing
    Solicitor::Client::ProfessionalExternalServices.stubs(:where).returns([])
    model = Profile::ScheduleForm.find(99999)
    refute model
  end

  def test_upsert_with_invalid_scheduling_url
    Solicitor::Client::ProfessionalExternalServices.expects(:create).never
    form = Profile::ScheduleForm.new(professional_id: @professional_id, scheduling_url: nil)
    form.upsert
    error_message = "The URL you entered is incorrect, please try again with the correct one."
    assert_equal(error_message, form.errors[:scheduling_url].first)
  end

  def test_upsert_with_invalid_response
    Solicitor::Client::ProfessionalExternalServices.expects(:create).returns(nil)
    form = Profile::ScheduleForm.new(professional_id: @professional_id, scheduling_url: @config_value)
    form.upsert
    error_message = "Scheduling URL could not be updated."
    assert_equal(error_message, form.errors[:scheduling_url].first)
  end

  def test_successful_upsert
    Solicitor::Client::ProfessionalExternalServices.expects(:create).returns(true)
    form = Profile::ScheduleForm.new(professional_id: @professional_id, scheduling_url: @config_value)
    assert form.upsert
  end
end
